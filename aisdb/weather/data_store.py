import datetime
import tempfile
import types
import xarray as xr
import numpy as np
import os
import shutil
from aisdb.database.decoder import fast_unzip
from aisdb.weather.utils import SHORT_NAMES_TO_VARIABLES
from aisdb.weather.weather_fetch import ClimateDataStore
from collections import defaultdict
import warnings
import glob
from concurrent.futures import ProcessPoolExecutor, as_completed
import uuid
from tqdm import tqdm
import hashlib
import pickle


def dt_to_iso8601(timestamp: float) -> str:
    """
    Convert a Unix epoch timestamp (seconds since epoch) to an ISO 8601 formatted string.

    Args:
        timestamp: Unix epoch timestamp.

    Returns:
        ISO 8601 formatted timestamp string.
    """
    dt = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
    # Ensure nanosecond precision for xarray time indexing compatibility
    iso_format = dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + '000'
    return iso_format


def get_monthly_range(start_date: datetime.datetime, end_date: datetime.datetime) -> list[str]:
    """
    Generate a list of month-year strings (e.g., '2023-01') between two given dates.

    Args:
        start_date: The start datetime object.
        end_date: The end datetime object.

    Returns:
        A list of strings representing the month-year range.
    """
    months = []
    current_date = start_date
    while current_date <= end_date:
        months.append(current_date.strftime('%Y-%m'))
        # Advance to the first day of the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1, day=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1, day=1)
    return list(set(months))  # Return unique months in case of overlapping start/end logic


# This function runs in worker processes. It should be self-contained or only use picklable arguments.
def _process_grib_file_for_variable(original_grib_file_path: str, short_name_target: str,
                                    month_str: str) -> dict | None:
    """
    Opens a GRIB file, extracts data for a specific short_name, and returns raw NumPy arrays.
    Designed to be run in a separate process to leverage multi-core processing.
    It copies the GRIB file to a temporary location to avoid index file conflicts between processes.
    """
    # Create a unique temporary directory for this specific task to avoid file collisions
    temp_grib_dir = tempfile.mkdtemp(prefix=f"grib_process_{short_name_target}_{uuid.uuid4().hex[:6]}_")
    temp_grib_file_path = os.path.join(temp_grib_dir, os.path.basename(original_grib_file_path))

    try:
        shutil.copy2(original_grib_file_path, temp_grib_file_path)  # Work on a copy

        # Suppress xarray's FutureWarning about decode_timedelta behavior change
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=FutureWarning, message=".*decode_timedelta.*")
            ds_filtered = xr.open_dataset(
                temp_grib_file_path,
                engine="cfgrib",
                backend_kwargs={'filter_by_keys': {'shortName': short_name_target}},
                decode_timedelta=False
            ).load()  # Load data into memory immediately as the temp file will be deleted

        if not ds_filtered.data_vars:
            return None  # No variables found for the given shortName filter

        # Identify the actual name of the variable in the dataset, as cfgrib might rename it
        actual_var_name_in_ds = None
        data_var_keys = list(ds_filtered.data_vars.keys())

        if short_name_target in data_var_keys:
            actual_var_name_in_ds = short_name_target
        elif len(data_var_keys) == 1:  # If only one variable, assume it's the correct one
            actual_var_name_in_ds = data_var_keys[0]
        else:  # Try matching by the long name defined in our utility mapping
            long_name_expected = SHORT_NAMES_TO_VARIABLES.get(short_name_target)
            if long_name_expected and long_name_expected in data_var_keys:
                actual_var_name_in_ds = long_name_expected

        if actual_var_name_in_ds is None: return None  # Variable could not be identified

        data_array = ds_filtered[actual_var_name_in_ds]

        # Standardize time coordinate name (e.g. 'valid_time' -> 'time')
        time_coord_name_in_ds = None
        if 'time' in data_array.coords:
            time_coord_name_in_ds = 'time'
        elif 'valid_time' in data_array.coords:
            time_coord_name_in_ds = 'valid_time'

        if not time_coord_name_in_ds: return None  # Essential time coordinate is missing

        # Ensure time coordinate is unique within this GRIB message/file part
        if not data_array.indexes[time_coord_name_in_ds].is_unique:
            _, unique_idx = np.unique(data_array[time_coord_name_in_ds], return_index=True)
            data_array = data_array.isel({time_coord_name_in_ds: unique_idx})

        # Extract all coordinate data as numpy arrays
        coords_data = {name: data_array[name].values for name in data_array.coords}

        # Ensure the primary time coordinate in our returned dict is always keyed as 'time'
        if time_coord_name_in_ds != 'time' and time_coord_name_in_ds in coords_data:
            coords_data['time'] = coords_data.pop(time_coord_name_in_ds)

        return {
            "short_name": short_name_target,
            "month_str": month_str,
            "data": data_array.values,
            "coords_data": coords_data,
            "dims": data_array.dims,
            "coords_attrs": {k: v.attrs for k, v in data_array.coords.items()},
            "data_attrs": data_array.attrs
        }
    except Exception:
        # print(f"Worker error processing {short_name_target} from {original_grib_file_path}: {e}")
        return None  # Return None on any processing error for this file/variable
    finally:
        try:
            shutil.rmtree(temp_grib_dir)  # Clean up temporary directory
        except Exception:
            pass


class WeatherDataStore:
    def __init__(self, short_names: list, start: datetime.datetime, end: datetime.datetime, weather_data_path: str,
                 download_from_cds: bool = False, num_workers: int = 8, use_cache: bool = True, **kwargs):
        """
        Initializes the WeatherDataStore.

        Args:
            short_names: List of weather variable short names (e.g., ['10u', '10v']).
            start: Start datetime for the weather data.
            end: End datetime for the weather data.
            weather_data_path: Path to the directory containing GRIB/ZIP files.
            download_from_cds: If True, download data from Copernicus Climate Data Store.
            num_workers: Number of parallel processes for loading GRIB data.
            use_cache: If True, try to load/save processed data from/to a pickle cache.
            **kwargs: Additional arguments for CDS download (e.g., 'area').
        """
        if not isinstance(short_names, list) or not all(isinstance(name, str) for name in short_names):
            raise ValueError("short_names should be a list of strings.")
        if not weather_data_path:
            raise ValueError("WEATHER_DATA_PATH is not specified.")

        self.start = start
        self.end = end
        self.months = get_monthly_range(start, end)
        self.num_workers = num_workers
        self.use_cache = use_cache

        self._check_available_short_names(short_names)
        self.short_names = sorted(list(set(short_names)))

        self.weather_data_path = weather_data_path
        os.makedirs(self.weather_data_path, exist_ok=True)

        self.cache_dir = os.path.join(self.weather_data_path, ".weather_cache")
        os.makedirs(self.cache_dir, exist_ok=True)

        # Generate a unique cache filename based on request parameters
        cache_filename_parts = [
            "_".join(self.short_names),
            self.start.strftime("%Y%m%d%H%M"),  # Include time for finer granularity if needed
            self.end.strftime("%Y%m%d%H%M"),
            os.path.basename(os.path.normpath(self.weather_data_path)),
            str(self.num_workers)
        ]
        cache_key = "-".join(cache_filename_parts)
        # Cache filename versioning can be useful if data processing logic changes
        self.cache_file_path = os.path.join(self.cache_dir,
                                            f"ds_map_cache_v3_{hashlib.md5(cache_key.encode()).hexdigest()}.pkl")

        if download_from_cds:
            self._download_data_from_cds(**kwargs)

        if self.use_cache and os.path.exists(self.cache_file_path):
            self._try_load_from_cache()
        else:
            print("INFO: Cache not used or cache file not found. Processing GRIB data...")
            self.weather_ds_map = self._load_weather_data()
            if self.use_cache:
                self._try_save_to_cache()

    def _download_data_from_cds(self, **kwargs):
        self.area = kwargs.get("area")
        if self.area is None or len(self.area) == 0:
            raise ValueError("""Missing parameter 'area' for CDS download.""")

        user_params = {
            "short_names": self.short_names,
            "start_time": self.start,
            "end_time": self.end,
            "area": self.area,
        }
        climateDataStore = ClimateDataStore(dataset="reanalysis-era5-single-levels", **user_params)
        print(f"Downloading weather data from CDS to: {self.weather_data_path}")
        climateDataStore.download_grib_file(output_folder=self.weather_data_path)

    def _try_load_from_cache(self):
        print(f"INFO: Loading weather_ds_map from cache: {self.cache_file_path}")
        try:
            with open(self.cache_file_path, 'rb') as f:
                self.weather_ds_map = pickle.load(f)
            print("INFO: Successfully loaded weather_ds_map from cache.")
        except Exception as e:
            print(f"Warning: Failed to load weather_ds_map from cache ({e}). Re-processing...")
            self.weather_ds_map = self._load_weather_data()
            if self.use_cache: self._try_save_to_cache()  # Attempt to save newly processed data

    def _try_save_to_cache(self):
        try:
            with open(self.cache_file_path, 'wb') as f:
                pickle.dump(self.weather_ds_map, f)
            print(f"INFO: Saved processed weather_ds_map to cache: {self.cache_file_path}")
        except Exception as e_save:
            print(f"Warning: Could not save processed weather_ds_map to cache: {e_save}")

    def extract_weather(self, latitude: float, longitude: float, time: float) -> dict:
        """
        Extracts weather data for a specific point and time using nearest-neighbor selection.
        """
        dt_iso_str = dt_to_iso8601(time)
        values = {}
        for shortname, ds in self.weather_ds_map.items():
            try:
                ds_to_select_from = ds
                # Ensure time index is unique for reliable selection
                if 'time' in ds_to_select_from.coords and not ds_to_select_from.indexes['time'].is_unique:
                    _, index = np.unique(ds_to_select_from['time'], return_index=True)
                    ds_to_select_from = ds_to_select_from.isel(time=index)

                selected_value = ds_to_select_from[shortname].sel(
                    latitude=latitude,
                    longitude=longitude,
                    time=dt_iso_str,
                    method='nearest'
                ).item()  # .item() to ensure scalar
                values[shortname] = selected_value
            except Exception:
                # print(f"Warning: extract_weather failed for {shortname} at {dt_iso_str} for lat/lon {latitude}/{longitude}: {e}")
                values[shortname] = np.nan
        return values

    def _load_weather_data(self) -> dict:
        grib_processing_dir = self.weather_data_path
        zipped_grib_files_to_extract = []

        # Phase 1: Prepare GRIB files (unzip if necessary)
        for month_str in self.months:
            grib_filename = f"{month_str}.grib"
            zip_filename = f"{grib_filename}.zip"
            full_grib_path = os.path.join(grib_processing_dir, grib_filename)
            full_zip_path = os.path.join(grib_processing_dir, zip_filename)

            if os.path.exists(full_zip_path):
                if not os.path.exists(full_grib_path) or \
                        (os.path.exists(full_grib_path) and os.path.getmtime(full_zip_path) > os.path.getmtime(
                            full_grib_path)):
                    zipped_grib_files_to_extract.append(full_zip_path)
            elif not os.path.exists(full_grib_path):
                print(f"Warning: GRIB data for month {month_str} not found at {full_grib_path} or as {full_zip_path}. ")
        if zipped_grib_files_to_extract:
            fast_unzip(zipped_grib_files_to_extract, grib_processing_dir)

        # Phase 2: Create tasks for parallel processing
        tasks = []
        for month_str in self.months:
            grib_filename = f"{month_str}.grib"
            grib_file_path = os.path.join(grib_processing_dir, grib_filename)
            if not os.path.exists(grib_file_path): continue
            for short_name in self.short_names:
                tasks.append((grib_file_path, short_name, month_str))

                # Phase 3: Execute tasks in parallel to extract raw data
        raw_data_parts_by_shortname = defaultdict(list)
        print(
            f"INFO: Starting parallel extraction of raw data from {len(tasks)} GRIB variable-month tasks using {self.num_workers} processes...")

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            future_to_task_info = {
                executor.submit(_process_grib_file_for_variable, task_args[0], task_args[1], task_args[2]): task_args
                for task_args in tasks
            }
            for future in tqdm(as_completed(future_to_task_info), total=len(tasks), desc="Extracting GRIB data"):
                original_grib_file, processed_short_name_key, month_str_of_data = future_to_task_info[future]
                try:
                    raw_data_dict = future.result()
                    if raw_data_dict:
                        raw_data_parts_by_shortname[raw_data_dict["short_name"]].append(raw_data_dict)
                except Exception as exc:
                    print(
                        f'Task for {processed_short_name_key} from {original_grib_file} generated an exception: {exc}')

        # Phase 4: Assemble raw data into xarray.Datasets in the main process
        print("INFO: Assembling and concatenating extracted GRIB data into xarray Datasets...")
        merged_per_shortname = {}
        for short_name, list_of_raw_data_dicts in tqdm(raw_data_parts_by_shortname.items(), desc="Assembling Datasets"):
            if not list_of_raw_data_dicts: continue
            list_of_raw_data_dicts.sort(key=lambda x: x["month_str"])

            all_data_arrays = [rd['data'] for rd in list_of_raw_data_dicts]
            all_time_arrays = [rd['coords_data']['time'] for rd in list_of_raw_data_dicts]

            concatenated_data = np.concatenate(all_data_arrays, axis=0)  # Assuming time is the first dimension
            concatenated_times = np.concatenate(all_time_arrays)

            first_part = list_of_raw_data_dicts[0]
            final_dims = first_part['dims']

            # Ensure unique time coordinates before creating DataArray
            unique_times, unique_indices = np.unique(concatenated_times, return_index=True)
            if len(unique_times) < len(concatenated_times):
                concatenated_data = concatenated_data[unique_indices]

            final_coords_constructor = {}
            for dim_name in final_dims:
                if dim_name == 'time':  # This is the concatenation dimension
                    final_coords_constructor[dim_name] = ('time', unique_times)
                elif dim_name in first_part['coords_data']:  # Other spatial/level dimensions
                    final_coords_constructor[dim_name] = (dim_name, first_part['coords_data'][dim_name])
                else:  # Should not happen if GRIB structure is consistent
                    print(
                        f"Warning: Dimension '{dim_name}' from GRIB not found in extracted coordinate data for {short_name}. Skipping this dimension for DataArray construction.")
            try:
                data_var = xr.DataArray(
                    concatenated_data,
                    coords=final_coords_constructor,
                    dims=final_dims,
                    name=short_name,
                    attrs=first_part['data_attrs']
                )
                for coord_name_attr, attrs in first_part['coords_attrs'].items():
                    if coord_name_attr in data_var.coords:
                        data_var.coords[coord_name_attr].attrs.update(attrs)

                merged_per_shortname[short_name] = xr.Dataset({short_name: data_var})
            except Exception as e:
                print(f"Warning: Could not create merged Dataset for {short_name}: {e}")

        if not merged_per_shortname and self.short_names:
            missing_vars = [sn for sn in self.short_names if sn not in merged_per_shortname]
            if missing_vars:
                print(f"Critical Error: No weather datasets for requested variables: {missing_vars}.")
            raise RuntimeError("Failed to load any weather datasets.")
        return merged_per_shortname

    def yield_tracks_with_weather(self, tracks) -> dict:
        assert isinstance(tracks, types.GeneratorType)
        for track in tracks:
            longitudes = np.array(track['lon'])
            latitudes = np.array(track['lat'])
            timestamps = np.array(track['time'])
            dt_iso_list = [dt_to_iso8601(t) for t in timestamps]

            unique_dt_iso_list, unique_indices = np.unique(dt_iso_list, return_index=True)

            if len(unique_dt_iso_list) == 0:
                track["weather_data"] = {sn: np.full(len(timestamps), np.nan) for sn in self.short_names}
                yield track
                continue

            sel_latitudes = latitudes[unique_indices]
            sel_longitudes = longitudes[unique_indices]

            lat_da = xr.DataArray(sel_latitudes, dims="points", name="latitude")
            lon_da = xr.DataArray(sel_longitudes, dims="points", name="longitude")
            time_da = xr.DataArray(np.array(unique_dt_iso_list, dtype='datetime64[ns]'), dims="points", name="time")

            weather_data_dict_unique_times = {}

            for short_name, ds in self.weather_ds_map.items():
                try:
                    if short_name in ds:
                        ds_to_select_from = ds
                        if 'time' in ds_to_select_from.coords and not ds_to_select_from.indexes['time'].is_unique:
                            _, index = np.unique(ds_to_select_from['time'], return_index=True)
                            ds_to_select_from = ds_to_select_from.isel(time=index)

                        selected_data_array = ds_to_select_from[short_name].sel(
                            latitude=lat_da,
                            longitude=lon_da,
                            time=time_da,
                            method="nearest"
                        )
                        selected_values = selected_data_array.values
                        if selected_values.ndim > 1:
                            selected_values = selected_values.squeeze()
                        if selected_values.ndim == 0:
                            selected_values = np.array([selected_values.item()])

                        weather_data_dict_unique_times[short_name] = selected_values

                    else:
                        weather_data_dict_unique_times[short_name] = np.full(len(unique_dt_iso_list), np.nan)
                except Exception as e:
                    # print(f"Warning: Failed to select {short_name} data for track (unique times): {e}")
                    weather_data_dict_unique_times[short_name] = np.full(len(unique_dt_iso_list), np.nan)

            final_weather_data_dict = {}
            for short_name, unique_values_arr in weather_data_dict_unique_times.items():
                full_values_arr = np.full(len(timestamps), np.nan)
                if unique_values_arr is not None and len(unique_values_arr) == len(unique_dt_iso_list):
                    time_to_val_map = {dt_str: val for dt_str, val in zip(unique_dt_iso_list, unique_values_arr)}
                    for i, original_dt_str in enumerate(dt_iso_list):
                        scalar_val = time_to_val_map.get(original_dt_str, np.nan)
                        if isinstance(scalar_val, np.ndarray) and scalar_val.size == 1:
                            full_values_arr[i] = scalar_val.item()
                        elif isinstance(scalar_val, (int, float, np.number)):
                            full_values_arr[i] = scalar_val
                        else:
                            full_values_arr[i] = np.nan
                final_weather_data_dict[short_name] = full_values_arr

            track["weather_data"] = final_weather_data_dict
            yield track

    def close(self):
        for _, ds in self.weather_ds_map.items():
            if isinstance(ds, xr.Dataset):
                ds.close()

    def _check_available_short_names(self, short_names):
        for short_name in short_names:
            value = SHORT_NAMES_TO_VARIABLES.get(short_name)
            if value is None or value == "":
                raise ValueError(f"Invalid shortName: {short_name}.")


DEFAULT_PARAMS = {
    "product_type": ["reanalysis"],
    "data_format": "grib",
    "download_format": "unarchived",
}

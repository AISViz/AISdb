##### aisdb/weather/data_store.py #####
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
from concurrent.futures import ProcessPoolExecutor, as_completed # Changed to ProcessPoolExecutor
import uuid 
from tqdm import tqdm 


def dt_to_iso8601(timestamp):
    """
    Convert a any timestamp to an ISO 8601 formatted string.

    Args:
        timestamp (float): Any timestamp (seconds since epoch).

    Returns:
        str: The timestamp in ISO 8601 format (e.g., '2025-01-29T12:34:56.000000000').

    Example:
        >>> dt_to_iso8601(1674963000)
        '2023-01-29T12:30:00.000000000'
    """

    dt = datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)
    iso_format = dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + '000'

    return iso_format
    
    
def get_monthly_range(start,end) -> list:
    """
    Generate a list of month-year strings between two given timestamps.

    Args:
        start: The start timestamp.
        end: The end timestamp.

    Returns:
        list: A list of strings representing the month-year range (e.g., ['2023-01', '2023-02']).

    Example:
        >>> get_monthly_range(1672531200, 1675123200)
        ['2023-01', '2023-02']
    """

    months = []
    current = start

    while current <= end:
        months.append(current.strftime('%Y-%m'))
        
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

class WeatherDataStore:
    def __init__(self,short_names: list, start:datetime.datetime,end: datetime.datetime, weather_data_path: str, download_from_cds: bool = False, num_workers: int = 8, **kwargs):
                
        if not isinstance(short_names, list) or not all(isinstance(name, str) for name in short_names):
            raise ValueError("short_names should be a list of strings.")
        
        if not weather_data_path:
            raise ValueError("WEATHER_DATA_PATH is not specified.")
        
        self.start = start
        self.end = end
        self.months = get_monthly_range(start, end) 
        self.num_workers = num_workers

        self._check_available_short_names(short_names)
       
        self.short_names = short_names

        self.weather_data_path = weather_data_path
        os.makedirs(self.weather_data_path, exist_ok=True)

        if download_from_cds == True:
            self.area = kwargs.get("area")
            if self.area  is None or len(self.area )==0:
                raise ValueError("""Missing parameter 'area'.""")
              
            user_params = {
                "short_names": self.short_names,
                "start_time": self.start,
                "end_time": self.end,
                "area": self.area,
            }

            climateDataStore = ClimateDataStore(dataset="reanalysis-era5-single-levels", **user_params)
            print(f"Downloading weather data from CDS to: {weather_data_path}")
            climateDataStore.download_grib_file(output_folder=weather_data_path)
        
        self.weather_ds_map = self._load_weather_data()
           
    def extract_weather(self, latitude, longitude, time) -> dict:
        dt = dt_to_iso8601(time)
        values = {}
        for shortname, ds in self.weather_ds_map.items():
            values[shortname] = ds[shortname].sel(
                latitude=latitude,
                longitude=longitude,
                time=dt,
                method='nearest'
            ).values
        return values

    def _process_grib_task(self, original_grib_file_path: str, short_name: str) -> tuple[str, xr.Dataset | None]:
        temp_grib_dir = tempfile.mkdtemp(prefix=f"grib_worker_{short_name}_{uuid.uuid4().hex[:6]}_")
        temp_grib_file_path = os.path.join(temp_grib_dir, os.path.basename(original_grib_file_path))
        
        dataset_result = None
        try:
            shutil.copy2(original_grib_file_path, temp_grib_file_path)
            
            ds_filtered = xr.open_dataset(
                temp_grib_file_path, 
                engine="cfgrib",
                backend_kwargs={'filter_by_keys': {'shortName': short_name}},
                decode_timedelta=False 
            )
            
            if not ds_filtered.data_vars:
                dataset_result = None
            else:
                actual_var_name = None
                if short_name in ds_filtered.data_vars:
                    actual_var_name = short_name
                else:
                    data_var_keys = list(ds_filtered.data_vars.keys())
                    if len(data_var_keys) == 1:
                        actual_var_name = data_var_keys[0]
                    else:
                        long_name_expected = SHORT_NAMES_TO_VARIABLES.get(short_name)
                        if long_name_expected in ds_filtered.data_vars:
                            actual_var_name = long_name_expected
                
                if actual_var_name is None:
                    dataset_result = None
                elif actual_var_name != short_name:
                    dataset_result = ds_filtered.rename({actual_var_name: short_name})
                else:
                    dataset_result = ds_filtered
                
        except Exception as e:
            # print(f"Warning: Worker failed for shortName '{short_name}' from {original_grib_file_path} (processing {temp_grib_file_path}): {e}")
            dataset_result = None 
        finally:
            try:
                shutil.rmtree(temp_grib_dir)
            except Exception as e_clean:
                print(f"Warning: Could not clean up temp directory {temp_grib_dir}: {e_clean}")
        
        return short_name, dataset_result


    def _load_weather_data(self) -> dict:
        grib_processing_dir = self.weather_data_path
        zipped_grib_files_to_extract = []

        all_idx_files = glob.glob(os.path.join(grib_processing_dir, "*.grib.*.idx"))
        all_idx_files += glob.glob(os.path.join(grib_processing_dir, "*.grib.idx"))
        for idx_file in all_idx_files:
            try:
                os.remove(idx_file)
            except OSError as e:
                print(f"Warning: Initial cleanup could not remove index file {idx_file}: {e}")


        for month_str in self.months:
            grib_filename = f"{month_str}.grib"
            zip_filename = f"{grib_filename}.zip"
            full_grib_path = os.path.join(grib_processing_dir, grib_filename)
            full_zip_path = os.path.join(grib_processing_dir, zip_filename)

            if os.path.exists(full_zip_path):
                if not os.path.exists(full_grib_path) or \
                   (os.path.exists(full_grib_path) and os.path.getmtime(full_zip_path) > os.path.getmtime(full_grib_path)):
                    zipped_grib_files_to_extract.append(full_zip_path)
            elif not os.path.exists(full_grib_path):
                 print(f"Warning: GRIB data for month {month_str} not found at {full_grib_path} or as {full_zip_path}. ")

        if zipped_grib_files_to_extract:
            fast_unzip(zipped_grib_files_to_extract, grib_processing_dir)

        tasks = []
        for month_str in self.months:
            grib_filename = f"{month_str}.grib"
            grib_file_path = os.path.join(grib_processing_dir, grib_filename)
            if not os.path.exists(grib_file_path):
                continue
            for short_name in self.short_names:
                tasks.append((grib_file_path, short_name, month_str)) 

        monthly_datasets_by_shortname = defaultdict(list)
        
        print(f"INFO: Starting parallel processing of {len(tasks)} GRIB variable-month tasks using {self.num_workers} processes...")
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=FutureWarning,
                message="In a future version of xarray decode_timedelta will default to False rather than None."
            )
            # Use ProcessPoolExecutor instead of ThreadPoolExecutor
            with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                future_to_task = {
                    executor.submit(self._process_grib_task, task[0], task[1]): task 
                    for task in tasks
                }
                for future in tqdm(as_completed(future_to_task), total=len(tasks), desc="Processing GRIB tasks"):
                    original_task_grib_file, original_task_short_name, original_month_str = future_to_task[future]
                    try:
                        processed_short_name, dataset = future.result()
                        if dataset is not None:
                            monthly_datasets_by_shortname[processed_short_name].append((original_month_str, dataset))
                    except Exception as exc:
                        print(f'Task for {original_task_short_name} from {original_task_grib_file} generated an exception: {exc}')
        
        merged_per_shortname = {}
        for short_name, month_ds_pairs in monthly_datasets_by_shortname.items():
            if not month_ds_pairs:
                continue
            month_ds_pairs.sort(key=lambda item: item[0])
            datasets_to_concat = [ds for _, ds in month_ds_pairs]
            try:
                merged = xr.concat(datasets_to_concat, dim="time")
                merged_per_shortname[short_name] = merged
            except Exception as e:
                print(f"Warning: Could not merge datasets for {short_name}: {e}")

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
            dt = [dt_to_iso8601(t) for t in timestamps]
            lat_da = xr.DataArray(latitudes, dims="points", name="latitude")
            lon_da = xr.DataArray(longitudes, dims="points", name="longitude")
            time_da = xr.DataArray(dt, dims="points", name="time")
            weather_data_dict = {}

            for short_name, ds in self.weather_ds_map.items():
                try:
                    if short_name in ds:
                        selected = ds[short_name].sel(
                            latitude=lat_da,
                            longitude=lon_da,
                            time=time_da,
                            method="nearest"
                        )
                        weather_data_dict[short_name] = selected.values
                    else:
                        weather_data_dict[short_name] = np.full(len(timestamps), np.nan)
                except Exception as e:
                    print(f"Warning: Failed to select {short_name} data for track: {e}")
                    weather_data_dict[short_name] = np.full(len(timestamps), np.nan)
            track["weather_data"] = weather_data_dict
            yield track

    def close(self):
        for _, ds in self.weather_ds_map.items():
            if isinstance(ds, xr.Dataset):
                ds.close()    
    def _check_available_short_names(self, short_names):        
        for short_name in short_names:
                value =  SHORT_NAMES_TO_VARIABLES.get(short_name)
                if value is None or value == "":
                    raise ValueError(f"Invalid shortName: {short_name}.")

DEFAULT_PARAMS = {
    "product_type": ["reanalysis"],
    "data_format": "grib",
    "download_format": "unarchived",
}

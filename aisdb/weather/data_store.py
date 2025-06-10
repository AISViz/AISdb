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
        # Format the current date as 'yyyy-mm'
        months.append(current.strftime('%Y-%m'))
        
        # Move to the next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

class WeatherDataStore:
    def __init__(self,short_names: list, start:datetime.datetime,end: datetime.datetime, weather_data_path: str, download_from_cds: bool = False,**kwargs):
        """
        Initialize a WeatherDataStore object to handle weather data extraction.

        Args:
            short_names (list): List of weather variable short names (e.g., ['10u', '10v']).
            start (datetime): Start date for the weather data.
            end (datetime): End date for the weather data.
            weather_data_path (str): Path to the directory containing weather data files.

        Example:
            >>> store = WeatherDataStore(['10u', '10v'], datetime.datetime(2023, 1, 1), datetime.datetime(2023, 2, 1), '/data/weather')
        
        Note:
            After using this object, make sure to call the `close` method to free resources.
        >>> store.close()
        """
                
        # Validate parameter_names
        if not isinstance(short_names, list) or not all(isinstance(name, str) for name in short_names):
            raise ValueError("short_names should be a list of strings.")
        
         # validate weather_data_path
        if weather_data_path =="":
            raise ValueError("WEATHER_DATA_PATH is not specified.")
        
        self.start = start
        self.end = end
        self.months = get_monthly_range(start, end) 

        self._check_available_short_names(short_names)
       
        self.short_names = short_names

        if weather_data_path =="":
            raise ValueError("WEATHER_DATA_PATH is not specified. WEATHER_DATA_PATH must be specified for either weather data extraction or to point to location where weather data grib file is placed")

        self.weather_data_path = weather_data_path

        if download_from_cds == True:
            self.area = kwargs.get("area")
            if self.area  == None or len(self.area )==0:
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
        """
        Extract weather data for a specific latitude, longitude, and timestamp.

        Args:
            latitude (float): Latitude of the point.
            longitude (float): Longitude of the point.
            time (int): Timestamp.

        Returns:
            dict: A dictionary containing the extracted weather data (e.g., {'10u': 5.2, '10v': 3.1}).

        Example:
            >>> store.extract_weather(40.7128, -74.0060, 1674963000)
            {'10u': 5.2, '10v': 3.1}
            >>> store.extract_weather(40.7128, -74.0060, 2023-05-01T12:00:00)
            {'10u': 5.2, '10v': 3.1}
        """

        dt = dt_to_iso8601(time)
        values = {}

        for short_name, ds in self.weather_ds_map.items(): 
            for var_da in ds.data_vars:
                selected = ds[var_da].sel(
                    latitude=latitude,
                    longitude=longitude,
                    time=dt,
                    method="nearest"
                )
                values[short_name] = selected.values
        return values
        
    def _load_weather_data(self) -> dict:
        """
        Load and extract weather data from GRIB files for the given date range,
        organized by shortName.

        Returns:
            dict: A dictionary where each key is a weather shortName and each value
                is an xarray.Dataset merged across all months for that variable.
        """
        from collections import defaultdict

        tmp_dir = tempfile.mkdtemp()
        zipped_grib_files = []

        # Collect zipped files or copy .grib directly
        for month in self.months:
            grib_path = f"{self.weather_data_path}/{month}.grib"
            zip_path = f"{grib_path}.zip"

            if os.path.exists(zip_path):
                zipped_grib_files.append(zip_path)
            elif os.path.exists(grib_path):
                shutil.copy(grib_path, f"{tmp_dir}/{month}.grib")
            else:
                raise FileNotFoundError(f"Neither {zip_path} nor {grib_path} found for month: {month}")

        if zipped_grib_files:
            fast_unzip(zipped_grib_files, tmp_dir)

        # Group datasets by shortName
        shortname_to_datasets = defaultdict(list)

        for month in self.months:
            grib_file_path = f"{tmp_dir}/{month}.grib"

            if not os.path.exists(grib_file_path):
                print(f"Warning: GRIB file not found: {grib_file_path}. Skipping.")
                continue

            for short_name in self.short_names:
                try:
                    ds = xr.open_dataset(
                        grib_file_path,
                        engine="cfgrib",
                        backend_kwargs={'filter_by_keys': {'shortName': short_name}}
                    )
                    shortname_to_datasets[short_name].append(ds)
                except Exception as e:
                    print(f"Warning: Failed to load {short_name} from {grib_file_path}: {e}")

        # Merge across time for each shortName
        merged_per_shortname = {}
        for short_name, datasets in shortname_to_datasets.items():
            try:
                merged = xr.concat(datasets, dim="time")
                merged_per_shortname[short_name] = merged
            except Exception as e:
                print(f"Warning: Could not merge datasets for {short_name}: {e}")


        if merged_per_shortname:
            return merged_per_shortname
        else:
            raise RuntimeError("No weather datasets could be loaded or merged.")


    def yield_tracks_with_weather(self, tracks) -> dict:
        """
        Yields tracks with weather by selecting weather variables for each point in the track.

        Args:
            tracks: A generator of dictionaries, where each dictionary
                represents a track and contains 'lon', 'lat', and 'time' keys.

        Yields:
            Track dictionaries with added 'weather_data' (a dict of variable → values).
        """
        assert isinstance(tracks, types.GeneratorType)

        for track in tracks:
            longitudes = np.array(track['lon'])
            latitudes = np.array(track['lat'])
            timestamps = np.array(track['time'])

            dt = [dt_to_iso8601(t) for t in timestamps]

            # Prepare selection coordinates
            lat_da = xr.DataArray(latitudes, dims="points", name="latitude")
            lon_da = xr.DataArray(longitudes, dims="points", name="longitude")
            time_da = xr.DataArray(dt, dims="points", name="time")

            weather_data_dict = {}

            # Iterate over the shortName → Dataset map
            for short_name, ds in self.weather_ds_map.items():
                try:
                    for var_da in ds.data_vars:
                        selected = ds[var_da].sel(
                            latitude=lat_da,
                            longitude=lon_da,
                            time=time_da,
                            method="nearest"
                        )
                        weather_data_dict[short_name] = selected.values
                except Exception as e:
                    print(f"Warning: Failed to select {short_name} data for track: {e}")
                    weather_data_dict[short_name] = [np.nan] * len(timestamps)

            track["weather_data"] = weather_data_dict
            yield track

    def close(self):
        """
        Close the weather dataset.
        """
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

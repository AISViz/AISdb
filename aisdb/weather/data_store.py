import datetime
import tempfile
import types
import xarray as xr
import numpy as np
from aisdb.database.decoder import fast_unzip
from aisdb.weather.utils import SHORT_NAMES_TO_VARIABLES
from aisdb.weather.weather_fetch import FetchWeather

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
                "variable": self.short_names,
                "start_time": self.start,
                "end_time": self.end,
                "area": self.area,
            }

            climateDataStore = FetchWeather(dataset="reanalysis-era5-single-levels", params_requested= user_params)

            print(f"Downloading weather data from CDS to: {weather_data_path}")

            climateDataStore.download_grib_file(output_path=weather_data_path)
        
        self.weather_ds = self._load_weather_data()
           
    def _load_weather_data(self):
        """
        Load and extract weather data from GRIB files for the given date range.

        Returns:
            xarray.Dataset: The concatenated dataset of weather data.
        """

        weather_dataset_instances = []

        # Create a temporary directory for extraction
        tmp_dir = tempfile.mkdtemp()
        zipped_grib_files = []
        
        for month in self.months:
            file_name = f"{self.weather_data_path}/{month}.grib"

            # Unzip the GRIB file
            zip_path = f"{file_name}.zip"
            zipped_grib_files.append(zip_path)

        fast_unzip(zipped_grib_files, tmp_dir) # TODO: optimize to do in the same loop

        for month in self.months:
            grib_file_path = f"{tmp_dir}/{month}.grib"
            
            # Load the weather dataset from the extracted GRIB file
            weather_ds = xr.open_dataset(
                grib_file_path,
                engine="cfgrib",
                backend_kwargs={
                    'filter_by_keys': {
                        'shortName': self.short_names,
                    }
                }
            )

            weather_dataset_instances.append(weather_ds)

        return xr.concat(weather_dataset_instances, dim='time')

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
        ds_variables = list(self.weather_ds.data_vars)
        values = {}

        # Loop through each variable (e.g., wind, wave)
        for var in ds_variables:
            values[var] = self.weather_ds[var].sel(latitude=latitude, longitude=longitude, time=dt, method='nearest').values

        return values
        
    def yield_tracks_with_weather(self, tracks) -> dict:
        """
        Yields tracks with weather. Takes a generator of track dictionaries, retrieves
        corresponding weather data from an xarray Dataset, and adds the
        weather data to each track.

        Args:
            tracks: A generator of dictionaries, where each dictionary
                represents a track and contains 'lon', 'lat', and 'time' keys
                with lists or numpy arrays of longitudes, latitudes, and
                timestamps, respectively.

        Yields:
            tracks: A generator of dictionaries, where each dictionary is a track
            with an added 'weather_data' key. The 'weather_data' value is
            a dictionary containing weather variable names.
        """
        assert isinstance(tracks, types.GeneratorType)

        for track in tracks:      
            longitudes = np.array(track['lon'])
            latitudes = np.array(track['lat'])
            timestamps = np.array(track['time'])
        
            dt = [dt_to_iso8601(t) for t in timestamps]
            
            # Initialize a dictionary to store weather data for each short name (weather variable)
            weather_data_dict = {short_name: [] for short_name in self.short_names}
            
            # Create xarray DataArrays for latitudes, longitudes, and times
            latitudes_da = xr.DataArray(latitudes, dims="points", name="latitude")
            longitudes_da = xr.DataArray(longitudes, dims="points", name="longitude")
            times_da = xr.DataArray(dt, dims="points", name="time")
            
            # Use xarray's multi-dimensional selection to get values for each variable
            for var in self.weather_ds.data_vars:
                # Extract values using xarray's .sel() for multi-dimensional indexing
                # We need to ensure that xarray knows the lat, lon, and time coordinates are aligned
                data = self.weather_ds[var].sel(
                    latitude=latitudes_da, 
                    longitude=longitudes_da, 
                    time=times_da, 
                    method="nearest"
                )
                
                
                weather_data_dict[var] = data.values 
            
            track["weather_data"] = weather_data_dict      
                  
            yield track
        


    def close(self):
        """
        Close the weather dataset.
        """
        self.weather_ds.close()
    
    def _check_available_short_names(self, short_names: list) -> list:        
        for i in short_names:
                value =  SHORT_NAMES_TO_VARIABLES.get(short_names[i])
                if value is None or value == "":
                    raise ValueError(f"Invalid shortName: {short_names[i]}.")
    
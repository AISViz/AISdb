import datetime
import os
import tempfile
import xarray as xr
import numpy as np
from aisdb.database.decoder import fast_unzip


def epoch_to_iso8601(epoch_time):
    """
    Convert epoch time to ISO 8601 format with nanoseconds.

    Parameters:
        epoch_time (int): The epoch time in seconds.

    Returns:
        str: The ISO 8601 formatted string with nanoseconds.
    """
    dt = datetime.datetime.fromtimestamp(epoch_time, datetime.UTC)
    # Format datetime object to ISO 8601 with nanoseconds (compatible with ERA-5 timestamp)
    iso_format = dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + '000'

    return iso_format


def epoch_to_datetime(epoch: np.uint32) -> datetime.datetime:
    """
    Converts epoch time (numpy.uint32) to a datetime.datetime object.

    Args:
        epoch (np.uint32): The epoch time in seconds.

    Returns:
        datetime.datetime: The corresponding datetime object.
    """
    return datetime.datetime.fromtimestamp(int(epoch))

    

def get_monthly_range(start: np.uint32, end: np.uint32) -> list:
    """
    Generates a list of 'yyyy-mm' values representing each month between the start and end dates (inclusive).

    Args:
        start (np.uint32): The start date in epoch seconds.
        end (np.uint32): The end date in epoch seconds.

    Returns:
        list: List of strings in the format 'yyyy-mm' for each month between start and end.
    """
    # Convert epoch to datetime
    start_dt = epoch_to_datetime(start)
    end_dt = epoch_to_datetime(end)

    months = []
    current = start_dt

    while current <= end_dt:
        # Format the current date as 'yyyy-mm'
        months.append(current.strftime('%Y-%m'))
        
        # Move to the next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

class ClimateDataStore:
    """
    Load weather data from a server and process it.
    Call WeatherDataFromServer.Close() to release resources after use.
    """
    def __init__(self,short_names: list, start:datetime.datetime,end: datetime.datetime, weather_data_path: str):
        # Validate parameter_names
        if not isinstance(short_names, list) or not all(isinstance(name, str) for name in short_names):
            raise ValueError("short_names should be a list of strings.")
        
        # validate weather_data_path
        if weather_data_path =="":
            raise ValueError("WEATHER_DATA_PATH is not specified.")
            
        self.start = start
        self.end = end
        self.months = get_monthly_range(start, end)
        self.short_names = short_names
        self.weather_data_path = weather_data_path

        self.weather_ds = self._load_weather_data()
           
    def _load_weather_data(self):
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
        Get the value of a weather variable at a specific latitude, longitude, and time.

        Args:
            lat (float): Latitude of the location.
            lon (float): Longitude of the location.
            epoch_time (int): Time in epoch seconds.

        Returns:
            float: Value of the variable at the given location and time.
        """
        # Convert epoch time to datetime object
        dt = epoch_to_iso8601(time)

        # Select the variable based on the short name -- example short_name '10u' has data_variable 'u10' which corresponds to 10-meter U-component wind velocity
        ds_variables = list(self.weather_ds.data_vars)

        # Initialize an empty dictionary to store values for each variable
        values = {}

        # Loop through each variable (e.g., wind, wave)
        for var in ds_variables:
            # Extract the value at the specified lat, lon, and time for each variable
            values[var] = self.weather_ds[var].sel(latitude=latitude, longitude=longitude, time=dt, method='nearest').values

        return values
        
    def extract_weather_multiple_points(self, latitudes, longitudes, timestamps) -> dict:
        # Convert epoch times to datetime objects
        dt = [epoch_to_iso8601(t) for t in timestamps]
        
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
            
            # Convert xarray DataArray to numpy array and store it
            # The shape of the data will be (len(latitudes),) if latitudes, longitudes, and times are 1D
            weather_data_dict[var] = data.values  # Extract values as numpy array
        
        return weather_data_dict

    def close(self):
        """
        Close the weather dataset.
        """
        self.weather_ds.close()
import datetime
import os
import tempfile
import xarray as xr
import numpy as np
from aisdb.database.decoder import fast_unzip

weather_data_path = os.getenv('WEATHER_DATA_PATH')

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

def get_monthly_range(start: datetime.datetime, end: datetime.datetime) -> list:
    """
    Generates a list of 'yyyy-mm' values representing each month between the start and end dates (inclusive).

    Args:
        start (datetime.datetime): The start date.
        end (datetime.datetime): The end date.

    Returns:
        list: List of strings in the format 'yyyy-mm' for each month between start and end.
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

class ClimateDataStore:
    """
    Load weather data from a server and process it.
    Call WeatherDataFromServer.Close() to release resources after use.
    """
    def __init__(self,short_names: list, start:datetime.datetime,end: datetime.datetime, weather_data_path: str = None):
        # Validate weather_data_path
        if weather_data_path is None:
            weather_data_path = os.getenv('WEATHER_DATA_PATH')
            if weather_data_path == "":
                raise ValueError("You must set the WEATHER_DATA_PATH environment variable, or pass it as an argument to the ClimateDataStore constructor.")
        
        # Validate parameter_names
        if not isinstance(short_names, list) or not all(isinstance(name, str) for name in short_names):
            raise ValueError("short_names should be a list of strings.")
        
        # Validate timestamp
        if not isinstance(start, datetime.datetime):
            raise ValueError("start_time should be a valid datetime object.")
        
        # Validate timestamp
        if not isinstance(end, datetime.datetime):
            raise ValueError("end_time should be a valid datetime object.")
            
        self.start = start
        self.end = end
        self.months = get_monthly_range(start, end)
        self.short_names = short_names
        self.weather_ds = self._load_weather_data()
        
        
    def _load_weather_data(self):
        """
        Fetch and process a GRIB file for specific weather parameters and a given time.

        Parameters:
            short_names (str): The short names of the weather parameters to extract.
            epoch_time (int): The epoch time in seconds to determine the time of the data.

        Returns:
            xarray.Dataset: The processed weather dataset containing the specified parameters.

        Notes:
            - GRIB file is packaged as a zip file with the same name plus a ".zip" extension.
            - Extracts the GRIB file to a temporary directory for processing.
        """
        weather_dataset_instances = []

        # Create a temporary directory for extraction
        tmp_dir = tempfile.mkdtemp()
        zipped_grib_files = []
        
        for month in self.months:
            file_name = f"{weather_data_path}/{month}.grib"

            # Unzip the GRIB file
            zip_path = f"{file_name}.zip"
            zipped_grib_files.append(zip_path)

        fast_unzip(zipped_grib_files, tmp_dir)

        for _ in self.months:
            # Load the weather dataset from the extracted GRIB file
            weather_ds = xr.open_dataset(
                os.path.join(tmp_dir, file_name),
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
        """
        Retrieve weather data for multiple geographical points and times.

        This method queries weather data for a list of latitude, longitude, and timestamp values,
        and aggregates the results into a dictionary of numpy arrays, where each key represents
        a weather variable (short name) and the value is an array of data for that variable.

        Args:
            latitudes (list[float]): A list of latitude values for the query points.
            longitudes (list[float]): A list of longitude values for the query points.
            timestamps (list[int]): A list of epoch timestamps (in seconds) for the query points.

        Returns:
            dict[str, numpy.ndarray]: A dictionary where:
                - The keys are short names of weather variables (e.g., '10u', '10v').
                - The values are numpy arrays containing the queried weather data for the corresponding variable.

        Raises:
            KeyError: If the queried weather variable is not available in the dataset.
            ValueError: If the lengths of `latitudes`, `longitudes`, and `timestamps` do not match.

        Example:
        ```
            latitudes = [45.0, 46.5, 47.2]
            longitudes = [-122.3, -123.5, -124.8]
            timestamps = [1672934400, 1672938000, 1672941600]

            result = climate_data_store.extract_weather_multiple_points(latitudes, longitudes, timestamps)
        ```
        Output:
            # result:
            # {
            #     '10u': array([5.2, 5.5, 5.8]),
            #     '10v': array([2.3, 2.7, 3.0])
            # }
        """
        # Initialize a dictionary to store weather data for each short name (weather variable)
        weather_data_dict = {short_name: [] for short_name in self.short_names}

        for i in range(len(longitudes)):
            # Retrieve the weather data for the current (latitude, longitude, time)
            weather_data = self.extract_weather(latitude=latitudes[i], longitude=longitudes[i], epoch_time=timestamps[i])

            for key, value in weather_data.items():
                weather_data_dict[key].append(value)

        # Convert lists to numpy arrays for each short name
        weather_numpy_dict = {short_name: np.array(weather_data_dict[short_name], dtype=float) 
                            for short_name in self.short_names}
        
        return weather_numpy_dict
    
    def close(self):
        """
        Close the weather dataset.
        """
        self.weather_ds.close()
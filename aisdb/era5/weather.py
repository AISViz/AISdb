import datetime
import xarray as xr
from aisdb.database.decoder import fast_unzip
import os
import tempfile

weather_data_path = os.getenv('WEATHER_DATA_PATH')  # Returns None if the variable is not set

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

class WeatherData:
    def __init__(self,short_names,epoch_time):
        self.short_names = short_names
        self.epoch_time = epoch_time
        self.weatherds = self._load_weather_data(short_names, epoch_time)

    def _load_weather_data(short_names: list, epoch_time) -> xr.core.dataset.Dataset:
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
        # Create a temporary directory for extraction
        tmp_dir = tempfile.mkdtemp()

        # Convert epoch time to ISO 8601 format and derive file name
        time_iso8601 = epoch_to_iso8601(epoch_time)
        yymm = time_iso8601[:7]  # Extract year and month (YYYY-MM)
        file_name = f"{weather_data_path}/{yymm}.grib"

        # Unzip the GRIB file
        zip_path = f"{file_name}.zip"
        fast_unzip(zip_path, tmp_dir)

        # Load the weather dataset from the extracted GRIB file
        weather_ds = xr.open_dataset(
            os.path.join(tmp_dir, file_name),
            engine="cfgrib",
            backend_kwargs={
                'filter_by_keys': {
                    'shortName': short_names,
                }
            }
        )

        return weather_ds


# Example usage
short_names = ['10u']
epoch_time = 1672531200 
weather_ds = WeatherData(short_names, epoch_time)
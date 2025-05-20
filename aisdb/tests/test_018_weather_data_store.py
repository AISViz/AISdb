import unittest
from datetime import datetime
from aisdb.weather.data_store import WeatherDataStore
from unittest.mock import patch, MagicMock
import xarray as xr
import numpy as np

class TestWeatherDataStore(unittest.TestCase):
    @patch("aisdb.weather.data_store.fast_unzip")  # Mocking the fast_unzip function to avoid actual file operations
    @patch("aisdb.weather.data_store.WeatherDataStore._load_weather_data")

    def test_initialization(self, mock_load_weather_data, mock_fast_unzip):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Mock fast_unzip to avoid actual unzipping
        mock_fast_unzip.return_value = None

        # Create a mock xarray.Dataset to simulate the expected behavior
        mock_ds = MagicMock(spec=xr.Dataset)

        # Mock 'data_vars' to simulate having some variables in the dataset
        mock_ds.data_vars = {'10u': MagicMock(), '10v': MagicMock()}

        # Mock the 'sel' method to return a mock object when called
        mock_sel = MagicMock()
        mock_sel.values = 5.2  # Simulate returning a value
        mock_ds['10u'].sel.return_value = mock_sel  # Ensure 'sel' method of '10u' returns this mock

        # Mock the _load_weather_data method to return the mock dataset
        mock_load_weather_data.return_value = mock_ds

        # Create an instance of WeatherDataStore
        store = WeatherDataStore(short_names, start, end, weather_data_path)

        # Assertions to verify correct initialization
        self.assertEqual(store.short_names, short_names)
        self.assertEqual(store.start, start)
        self.assertEqual(store.end, end)
        self.assertEqual(store.weather_data_path, weather_data_path)

        # Check that weather_ds is a valid xarray Dataset
        self.assertIsInstance(store.weather_ds, xr.Dataset)

        # Check that the weather dataset is not empty
        self.assertTrue(len(store.weather_ds.data_vars) > 0)

        mock_load_weather_data.assert_called_once()

    @patch("aisdb.weather.data_store.WeatherDataStore._load_weather_data")  # Mock the method to avoid loading real data
    def test_extract_weather(self, mock_load_weather_data):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Mock _load_weather_data to return a mock dataset
        mock_load_weather_data.return_value = MagicMock()

        # Create an instance of WeatherDataStore
        store = WeatherDataStore(short_names, start, end, weather_data_path)

        # Mock dataset and the 'data_vars' for both '10u' and '10v'
        mock_values = {'10u': 5.2}  # Mock values based on the variable names
        store.weather_ds = MagicMock()

        # Mock the 'data_vars' to contain '10u' and '10v'
        store.weather_ds.data_vars = {'10u': MagicMock(), '10v': MagicMock()}

        # Mock the 'sel' method for both variables to return the mock values directly
        store.weather_ds['10u'].sel = MagicMock(return_value=MagicMock(values=mock_values['10u']))

        # Test extracting weather data
        weather = store.extract_weather(40.7128, -74.0060, 1674963000)

        # Assert that the correct values were extracted
        self.assertEqual(weather['10u'], 5.2)  # Ensure 10u has the correct mocked value

    def test_yield_tracks_with_weather(self):
        # Create a mock dataset with weather variables
        mock_ds = MagicMock(spec=xr.Dataset)
        mock_ds.data_vars = {'10u': MagicMock(), '10v': MagicMock()}
        
        # Mock .sel() return values for 10u and 10v
        mock_ds['10u'].sel.return_value = MagicMock(values=np.array([5.2, -1.3]))  # Expected for 10u

        # Setup test data
        short_names = ['10u']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/fake/path'

        with patch.object(WeatherDataStore, '_load_weather_data', return_value=mock_ds):
            store = WeatherDataStore(short_names, start, end, weather_data_path)

            # Create a sample track generator with timestamps (integers)
            def track_generator():
                yield {
                    'lon': [10.0, 20.0],
                    'lat': [30.0, 40.0],
                    'time': [1672531200, 1675123200]  # UNIX timestamps
                }

            # Call the method under test
            tracks_with_weather = list(store.yield_tracks_with_weather(track_generator()))

            # Validate the output
            self.assertEqual(len(tracks_with_weather), 1)
            self.assertIn('weather_data', tracks_with_weather[0])

            # Ensure correct values are returned
            np.testing.assert_array_equal(tracks_with_weather[0]['weather_data']['10u'], [5.2, -1.3])

            # Ensure the 'sel' method was called
            mock_ds['10u'].sel.assert_called()
            mock_ds['10v'].sel.assert_called()

        store.close()
        

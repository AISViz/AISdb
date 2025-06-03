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

        # Create mock datasets for each shortname
        mock_ds_10u = MagicMock(spec=xr.Dataset)
        mock_ds_10v = MagicMock(spec=xr.Dataset)

        # Compose the dict that WeatherDataStore expects
        mock_weather_map = {
            '10u': mock_ds_10u,
            '10v': mock_ds_10v
        }

        # Mock the _load_weather_data method to return the dict
        mock_load_weather_data.return_value = mock_weather_map

        # Create the WeatherDataStore instance
        store = WeatherDataStore(short_names, start, end, weather_data_path)

        # Assertions
        self.assertEqual(store.short_names, short_names)
        self.assertEqual(store.start, start)
        self.assertEqual(store.end, end)
        self.assertEqual(store.weather_data_path, weather_data_path)

        self.assertIsInstance(store.weather_ds_map, dict)
        self.assertIn('10u', store.weather_ds_map)
        self.assertIn('10v', store.weather_ds_map)
        self.assertIsInstance(store.weather_ds_map['10u'], xr.Dataset)
        self.assertIsInstance(store.weather_ds_map['10v'], xr.Dataset)

        mock_load_weather_data.assert_called_once()


    @patch("aisdb.weather.data_store.WeatherDataStore._load_weather_data")  # Mock the method to avoid loading real data
    def test_extract_weather(self, mock_load_weather_data):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Create mock variable datasets for 10u and 10v
        mock_10u_var = MagicMock()
        mock_10u_var.sel.return_value.values = 5.2

        mock_10u_ds = { '10u': mock_10u_var }

        mock_10v_var = MagicMock()
        mock_10v_var.sel.return_value.values = 3.1

        mock_10v_ds = { '10v': mock_10v_var }

        # Simulate the weather_ds_map with each shortname pointing to its own dataset
        mock_weather_ds_map = {
            '10u': mock_10u_ds,
            '10v': mock_10v_ds,
        }

        # Patch _load_weather_data to return our mocked weather_ds_map
        mock_load_weather_data.return_value = mock_weather_ds_map

        # Create an instance of WeatherDataStore
        store = WeatherDataStore(short_names, start, end, weather_data_path)

        # Inject a mock method since extract_weather uses ds[shortname].sel(...)
        for shortname in short_names:
            ds_mock = MagicMock()
            ds_mock.__getitem__.return_value.sel.return_value.values = 5.2 if shortname == '10u' else 3.1
            store.weather_ds_map[shortname] = ds_mock

        # Test extracting weather data
        weather = store.extract_weather(40.7128, -74.0060, 1674963000)

        # Assert that the correct values were extracted
        self.assertEqual(weather['10u'], 5.2)
        self.assertEqual(weather['10v'], 3.1)


    def test_yield_tracks_with_weather(self):
        # Create mock datasets for '10u' and '10v'
        mock_ds_10u = MagicMock()
        mock_ds_10u.sel.side_effect = [
            MagicMock(values=np.array(5.2)),
            MagicMock(values=np.array(-1.3))
        ]

        mock_ds_10v = MagicMock()
        mock_ds_10v.sel.side_effect = [
            MagicMock(values=np.array(1.1)),
            MagicMock(values=np.array(-0.8))
        ]

        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/fake/path'

        # Patch _load_weather_data to return a dict of shortnames to mocked datasets
        with patch.object(WeatherDataStore, '_load_weather_data', return_value={
            '10u': mock_ds_10u,
            '10v': mock_ds_10v
        }):
            store = WeatherDataStore(short_names, start, end, weather_data_path)

            # Create a sample track generator
            def track_generator():
                yield {
                    'lon': [10.0, 20.0],
                    'lat': [30.0, 40.0],
                    'time': [1672531200, 1675123200]  # UNIX timestamps
                }

            # Patch extract_weather to use the mocked sel values directly
            tracks_with_weather = list(store.yield_tracks_with_weather(track_generator()))

            # Validate output
            self.assertEqual(len(tracks_with_weather), 1)
            self.assertIn('weather_data', tracks_with_weather[0])

            np.testing.assert_array_equal(tracks_with_weather[0]['weather_data']['10u'], [5.2, -1.3])
            np.testing.assert_array_equal(tracks_with_weather[0]['weather_data']['10v'], [1.1, -0.8])

            # Ensure sel was called expected number of times
            self.assertEqual(mock_ds_10u.sel.call_count, 2)
            self.assertEqual(mock_ds_10v.sel.call_count, 2)

            store.close()


        

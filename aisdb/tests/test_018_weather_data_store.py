import unittest
from datetime import datetime
from aisdb.weather.data_store import WeatherDataStore
from unittest.mock import patch, MagicMock
import xarray as xr
import numpy as np
import os

class TestWeatherDataStore(unittest.TestCase):
    @patch("aisdb.weather.data_store.fast_unzip")
    @patch("aisdb.weather.data_store.WeatherDataStore._load_weather_data")
    def test_initialization(self, mock_load_weather_data, mock_fast_unzip, tmp_path):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        # Use tmp_path for weather_data_path
        weather_data_path = tmp_path / "weather_data"

        mock_fast_unzip.return_value = None

        mock_ds_10u = MagicMock(spec=xr.Dataset)
        mock_ds_10v = MagicMock(spec=xr.Dataset)
        mock_weather_map = {'10u': mock_ds_10u, '10v': mock_ds_10v}
        mock_load_weather_data.return_value = mock_weather_map

        store = WeatherDataStore(short_names, start, end, str(weather_data_path)) # Pass as string

        self.assertEqual(store.short_names, short_names)
        self.assertEqual(store.start, start)
        self.assertEqual(store.end, end)
        self.assertEqual(store.weather_data_path, str(weather_data_path)) # Compare as string

        self.assertIsInstance(store.weather_ds_map, dict)
        self.assertIn('10u', store.weather_ds_map)
        self.assertIn('10v', store.weather_ds_map)
        self.assertIsInstance(store.weather_ds_map['10u'], xr.Dataset)
        self.assertIsInstance(store.weather_ds_map['10v'], xr.Dataset)

        mock_load_weather_data.assert_called_once()


    @patch("aisdb.weather.data_store.WeatherDataStore._load_weather_data")
    def test_extract_weather(self, mock_load_weather_data, tmp_path):
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = tmp_path / "weather_data"

        mock_10u_var = MagicMock()
        mock_10u_var.sel.return_value.item.return_value = 5.2
        mock_10u_ds = { '10u': mock_10u_var }

        mock_10v_var = MagicMock()
        mock_10v_var.sel.return_value.item.return_value = 3.1
        mock_10v_ds = { '10v': mock_10v_var }

        mock_weather_ds_map = {'10u': mock_10u_ds, '10v': mock_10v_ds}
        mock_load_weather_data.return_value = mock_weather_ds_map

        store = WeatherDataStore(short_names, start, end, str(weather_data_path))

        # Mocking how ds[shortname].sel(...).item() would behave
        # The WeatherDataStore's extract_weather now uses .item()
        mock_ds_10u_instance = MagicMock(spec=xr.Dataset)
        mock_data_array_10u = MagicMock(spec=xr.DataArray)
        mock_data_array_10u.sel.return_value.item.return_value = 5.2
        mock_ds_10u_instance.__getitem__.return_value = mock_data_array_10u

        mock_ds_10v_instance = MagicMock(spec=xr.Dataset)
        mock_data_array_10v = MagicMock(spec=xr.DataArray)
        mock_data_array_10v.sel.return_value.item.return_value = 3.1
        mock_ds_10v_instance.__getitem__.return_value = mock_data_array_10v

        store.weather_ds_map = {'10u': mock_ds_10u_instance, '10v': mock_ds_10v_instance}


        weather = store.extract_weather(40.7128, -74.0060, 1674963000)

        self.assertEqual(weather['10u'], 5.2)
        self.assertEqual(weather['10v'], 3.1)


    @patch("aisdb.weather.data_store.WeatherDataStore._load_weather_data")
    def test_yield_tracks_with_weather(self, mock_load_weather_data, tmp_path):
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = tmp_path / "fake_weather_data"

        mock_ds_10u_sel_result = MagicMock(spec=xr.DataArray)
        mock_ds_10u_sel_result.values = np.array([5.2, 5.3])
        mock_ds_10u_var = MagicMock(spec=xr.DataArray)
        mock_ds_10u_var.sel.return_value = mock_ds_10u_sel_result
        mock_ds_10u = MagicMock(spec=xr.Dataset)
        mock_ds_10u.__contains__.side_effect = lambda key: key == '10u'
        mock_ds_10u.__getitem__.return_value = mock_ds_10u_var
        mock_ds_10u.indexes = {'time': MagicMock(is_unique=True)}


        mock_ds_10v_sel_result = MagicMock(spec=xr.DataArray)
        mock_ds_10v_sel_result.values = np.array([1.1, 1.2])
        mock_ds_10v_var = MagicMock(spec=xr.DataArray)
        mock_ds_10v_var.sel.return_value = mock_ds_10v_sel_result
        mock_ds_10v = MagicMock(spec=xr.Dataset)
        mock_ds_10v.__contains__.side_effect = lambda key: key == '10v'
        mock_ds_10v.__getitem__.return_value = mock_ds_10v_var
        mock_ds_10v.indexes = {'time': MagicMock(is_unique=True)}


        mock_load_weather_data.return_value={
            '10u': mock_ds_10u,
            '10v': mock_ds_10v
        }

        store = WeatherDataStore(short_names, start, end, str(weather_data_path))

        def track_generator():
            yield {
                'lon': np.array([10.0, 20.0]),
                'lat': np.array([30.0, 40.0]),
                'time': np.array([1672531200, 1675123200])
            }

        tracks_with_weather = list(store.yield_tracks_with_weather(track_generator()))

        self.assertEqual(len(tracks_with_weather), 1)
        self.assertIn('weather_data', tracks_with_weather[0])
        np.testing.assert_array_equal(tracks_with_weather[0]['weather_data']['10u'], [5.2, 5.3])
        np.testing.assert_array_equal(tracks_with_weather[0]['weather_data']['10v'], [1.1, 1.2])

        # Ensure sel was called for each unique time point group
        self.assertEqual(mock_ds_10u_var.sel.call_count, 1)
        self.assertEqual(mock_ds_10v_var.sel.call_count, 1)

        store.close()

if __name__ == "__main__":
    unittest.main()

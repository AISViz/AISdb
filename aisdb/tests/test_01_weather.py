import unittest
import datetime
import tempfile
import os
import numpy as np
from unittest.mock import patch, MagicMock
from aisdb.weather.era5 import epoch_to_iso8601, get_monthly_range, ClimateDataStore

class TestEpochToISO8601(unittest.TestCase):
    def test_epoch_to_iso8601(self):
        epoch_time = 1672531200  # Corresponds to 2023-01-01T00:00:00Z
        expected = "2023-01-01T00:00:00.000000000"
        self.assertEqual(epoch_to_iso8601(epoch_time), expected)

class TestGetMonthlyRange(unittest.TestCase):
    def test_get_monthly_range(self):
        start = datetime.datetime(2023, 1, 1)
        end = datetime.datetime(2023, 3, 1)
        expected = ["2023-01", "2023-02", "2023-03"]
        self.assertEqual(get_monthly_range(start, end), expected)

    def test_get_monthly_range_same_month(self): 
        start = datetime.datetime(2023, 1, 1)
        end = datetime.datetime(2023, 1, 31)
        expected = ["2023-01"]
        self.assertEqual(get_monthly_range(start, end), expected)

class TestClimateDataStore(unittest.TestCase):
    @patch("aisdb.weather.era5.fast_unzip")
    @patch("xarray.open_dataset")
    @patch("os.getenv")
    def test_init(self, mock_getenv, mock_open_dataset, mock_fast_unzip):
        mock_getenv.return_value = tempfile.mkdtemp()
        mock_open_dataset.return_value = MagicMock()
        mock_fast_unzip.return_value = None

        short_names = ["10u", "10v"]
        start = datetime.datetime(2023, 1, 1)
        end = datetime.datetime(2023, 1, 31)

        cds = ClimateDataStore(short_names, start, end)

        self.assertEqual(cds.start, start)
        self.assertEqual(cds.end, end)
        self.assertEqual(cds.short_names, short_names)
        self.assertEqual(cds.months, ["2023-01"])

    @patch("xarray.open_dataset")
    @patch("aisdb.weather.era5.epoch_to_iso8601")
    def test_extract_weather(self, mock_epoch_to_iso8601, mock_open_dataset):
        mock_open_dataset.return_value = MagicMock()
        mock_epoch_to_iso8601.return_value = "2023-01-01T00:00:00.000000000"

        cds = ClimateDataStore(["10u"], datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 31))
        cds.weather_ds = MagicMock()
        cds.weather_ds.data_vars = ["10u"]
        cds.weather_ds["10u"].sel.return_value.values = 5.2

        result = cds.extract_weather(45.0, -122.3, 1672531200)

        self.assertEqual(result, {"10u": 5.2})

    @patch("xarray.open_dataset")
    def test_extract_weather_multiple_points(self, mock_open_dataset):
        mock_open_dataset.return_value = MagicMock()

        cds = ClimateDataStore(["10u", "10v"], datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 31))
        cds.extract_weather = MagicMock()
        cds.extract_weather.side_effect = [
            {"10u": 5.2, "10v": 2.3},
            {"10u": 5.5, "10v": 2.7},
            {"10u": 5.8, "10v": 3.0},
        ]

        latitudes = [45.0, 46.5, 47.2]
        longitudes = [-122.3, -123.5, -124.8]
        timestamps = [1672531200, 1672534800, 1672538400]

        result = cds.extract_weather_multiple_points(latitudes, longitudes, timestamps)

        np.testing.assert_array_equal(result["10u"], np.array([5.2, 5.5, 5.8]))
        np.testing.assert_array_equal(result["10v"], np.array([2.3, 2.7, 3.0]))

    @patch("xarray.open_dataset")
    def test_close(self, mock_open_dataset):
        mock_open_dataset.return_value = MagicMock()

        cds = ClimateDataStore(["10u"], datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 31))
        cds.weather_ds = MagicMock()
        cds.close()

        cds.weather_ds.close.assert_called_once()

if __name__ == "__main__":
    unittest.main()

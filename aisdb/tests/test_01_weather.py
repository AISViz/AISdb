import unittest
from datetime import datetime
from weather.era5 import ClimateDataStore
from unittest.mock import patch, MagicMock

class TestClimateDataStore(unittest.TestCase):
    @patch("weather.era5.fast_unzip")  # Mocking the fast_unzip function to avoid actual file operations
    def test_initialization(self, mock_fast_unzip):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Mock fast_unzip to avoid actual unzipping
        mock_fast_unzip.return_value = None

        # Create an instance of ClimateDataStore
        store = ClimateDataStore(short_names, start, end, weather_data_path)

        # Test that initialization works
        self.assertEqual(store.short_names, short_names)
        self.assertEqual(store.start, start)
        self.assertEqual(store.end, end)
        self.assertEqual(store.weather_data_path, weather_data_path)
        self.assertTrue(len(store.weather_ds) > 0)  # Check that the weather dataset is not empty

    @patch("weather.era5.xr.open_dataset")  # Mocking xarray to avoid actual file reading
    def test_load_weather_data(self, mock_open_dataset):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Mock open_dataset to return a mock dataset
        mock_dataset = MagicMock()
        mock_open_dataset.return_value = mock_dataset

        # Create an instance of ClimateDataStore
        store = ClimateDataStore(short_names, start, end, weather_data_path)

        # Test if the _load_weather_data method correctly loads the weather dataset
        store._load_weather_data()

        mock_open_dataset.assert_called()  # Check that the open_dataset method was called

    @patch("weather.era5.ClimateDataStore._load_weather_data")  # Mock the method to avoid loading real data
    def test_extract_weather(self, mock_load_weather_data):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Mock _load_weather_data to return a mock dataset
        mock_load_weather_data.return_value = MagicMock()

        # Create an instance of ClimateDataStore
        store = ClimateDataStore(short_names, start, end, weather_data_path)

        # Setup mock return value for weather extraction
        mock_values = {'10u': 5.2, '10v': 3.1}
        store.weather_ds = MagicMock()
        store.weather_ds.data_vars = ['10u', '10v']
        store.weather_ds['10u'].sel.return_value.values = 5.2
        store.weather_ds['10v'].sel.return_value.values = 3.1

        # Test extracting weather data
        weather = store.extract_weather(40.7128, -74.0060, 1674963000)

        # Assert that the correct values were extracted
        self.assertEqual(weather['10u'], 5.2)
        self.assertEqual(weather['10v'], 3.1)

    @patch("weather.era5.xr.DataArray")  # Mock xarray DataArray to prevent actual data handling
    def test_extract_weather_multiple_points(self, mock_data_array):
        # Setup test data
        short_names = ['10u', '10v']
        start = datetime(2023, 1, 1)
        end = datetime(2023, 2, 1)
        weather_data_path = '/path/to/weather/data'

        # Mock DataArray creation and values extraction
        mock_data = MagicMock()
        mock_data.values = [5.2, 3.1]
        mock_data_array.return_value = mock_data

        # Create an instance of ClimateDataStore
        store = ClimateDataStore(short_names, start, end, weather_data_path)

        # Test extracting weather data for multiple points
        latitudes = [40.7128, 34.0522]
        longitudes = [-74.0060, -118.2437]
        timestamps = [1674963000, 1675049400]
        weather_data = store.extract_weather_multiple_points(latitudes, longitudes, timestamps)

        # Assert the correct values are returned
        self.assertEqual(weather_data['10u'], [5.2, 5.2])  # Assuming mock data returns 5.2 for both points
        self.assertEqual(weather_data['10v'], [3.1, 3.1])  # Assuming mock data returns 3.1 for both points

    def test_invalid_initialization(self):
        # Test invalid short names
        with self.assertRaises(ValueError):
            store = ClimateDataStore(['invalid_name'], datetime(2023, 1, 1), datetime(2023, 2, 1), '/data/weather')

        # Test invalid weather_data_path
        with self.assertRaises(ValueError):
            store = ClimateDataStore(['10u', '10v'], datetime(2023, 1, 1), datetime(2023, 2, 1), '')

if __name__ == "__main__":
    unittest.main()
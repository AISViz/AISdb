import unittest
from datetime import datetime
from aisdb.weather.era5 import ClimateDataStore
from unittest.mock import patch, MagicMock
import xarray as xr

class TestClimateDataStore(unittest.TestCase):
    @patch("aisdb.weather.era5.fast_unzip")  # Mocking the fast_unzip function to avoid actual file operations
    @patch("xarray.open_dataset")  # Mocking xarray's open_dataset to avoid reading actual files
    @patch("xarray.concat")  # Mocking xarray's concat to avoid actual concatenation
    def test_initialization(self, mock_concat, mock_open_dataset, mock_fast_unzip):
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
        
        # Mock open_dataset to return our mock dataset
        mock_open_dataset.return_value = mock_ds

        # Mock concat to return our mock dataset as if it's concatenated from multiple datasets
        # Ensuring we correctly simulate the typing (T_Dataset)
        mock_concat.return_value = mock_ds

        # Create an instance of ClimateDataStore
        store = ClimateDataStore(short_names, start, end, weather_data_path)

        # Test that initialization works
        self.assertEqual(store.short_names, short_names)
        self.assertEqual(store.start, start)
        self.assertEqual(store.end, end)
        self.assertEqual(store.weather_data_path, weather_data_path)
        
        # Check that weather_ds is a valid xarray Dataset
        self.assertIsInstance(store.weather_ds, xr.Dataset)
        
        # Check that the weather dataset is not empty
        self.assertTrue(len(store.weather_ds.data_vars) > 0)  # The mock object should have data_vars
        
        # Assert that xarray.open_dataset was called to load the mock dataset
        mock_open_dataset.assert_called()

        # Assert that xarray.concat was called to combine the datasets
        mock_concat.assert_called()

    @patch("aisdb.weather.era5.ClimateDataStore._load_weather_data")  # Mock the method to avoid loading real data
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
        
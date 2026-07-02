from datetime import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from aisdb.weather.data_store import WeatherDataStore

SHORT_NAMES = ["10u", "10v"]
START = datetime(2023, 1, 1)
END = datetime(2023, 2, 1)
WEATHER_DATA_PATH = "/path/to/weather/data"


@pytest.fixture
def make_store():
    def _make(weather_ds_map, short_names=None):
        with patch.object(
            WeatherDataStore, "_load_weather_data", return_value=weather_ds_map
        ) as mock_load:
            store = WeatherDataStore(
                short_names or SHORT_NAMES, START, END, WEATHER_DATA_PATH
            )
        mock_load.assert_called_once()
        return store

    return _make


def _mock_dataset_with_vars(var_names, values):
    ds = MagicMock()
    ds.data_vars = list(var_names)
    selected = MagicMock()
    selected.values = values
    ds.__getitem__.return_value.sel.return_value = selected
    return ds


def test_initialization(make_store):
    weather_ds_map = {
        "10u": MagicMock(spec=xr.Dataset),
        "10v": MagicMock(spec=xr.Dataset),
    }
    store = make_store(weather_ds_map)

    assert store.short_names == SHORT_NAMES
    assert store.start == START
    assert store.end == END
    assert store.weather_data_path == WEATHER_DATA_PATH
    assert isinstance(store.weather_ds_map, dict)
    assert set(store.weather_ds_map.keys()) == {"10u", "10v"}
    assert isinstance(store.weather_ds_map["10u"], xr.Dataset)
    assert isinstance(store.weather_ds_map["10v"], xr.Dataset)


@pytest.mark.parametrize(
    "short_names",
    [
        "10u",
        [10, "10v"],
        ["not-a-real-short-name"],
    ],
)
def test_invalid_short_names_raise(short_names):
    with pytest.raises(ValueError):
        WeatherDataStore(short_names, START, END, WEATHER_DATA_PATH)


def test_empty_weather_data_path_raises():
    with pytest.raises(ValueError):
        WeatherDataStore(SHORT_NAMES, START, END, "")


@pytest.mark.parametrize(
    "short_name,expected", [("10u", 5.2), ("10v", 3.1)], ids=["10u", "10v"]
)
def test_extract_weather(make_store, short_name, expected):
    weather_ds_map = {
        "10u": _mock_dataset_with_vars(["10u"], 5.2),
        "10v": _mock_dataset_with_vars(["10v"], 3.1),
    }
    store = make_store(weather_ds_map)

    weather = store.extract_weather(40.7128, -74.0060, 1674963000)

    assert weather[short_name] == expected


def test_extract_weather_var_name_differs_from_shortname(make_store):
    # regression test for the shortName KeyError fix (PR #151): grib
    # variables are often exposed under a different name than the requested
    # shortName (e.g. '10u' loads as 'u10'), so extract_weather must iterate
    # ds.data_vars instead of indexing the dataset with the shortName
    ds = _mock_dataset_with_vars(["u10"], 5.2)
    store = make_store({"10u": ds}, short_names=["10u"])

    weather = store.extract_weather(40.7128, -74.0060, 1674963000)

    assert weather["10u"] == 5.2
    ds.__getitem__.assert_called_once_with("u10")


def test_yield_tracks_with_weather(make_store):
    mock_ds_10u = MagicMock()
    mock_ds_10u.data_vars = ["u10"]
    mock_ds_10u.__getitem__.return_value.sel.side_effect = [
        MagicMock(values=np.array(5.2)),
    ]

    mock_ds_10v = MagicMock()
    mock_ds_10v.data_vars = ["v10"]
    mock_ds_10v.__getitem__.return_value.sel.side_effect = [
        MagicMock(values=np.array(1.1)),
    ]

    store = make_store({"10u": mock_ds_10u, "10v": mock_ds_10v})

    def track_generator():
        yield {
            "lon": [10.0, 20.0],
            "lat": [30.0, 40.0],
            "time": [1672531200, 1675123200],
        }

    tracks_with_weather = list(store.yield_tracks_with_weather(track_generator()))

    assert len(tracks_with_weather) == 1
    assert "weather_data" in tracks_with_weather[0]
    np.testing.assert_array_equal(tracks_with_weather[0]["weather_data"]["10u"], [5.2])
    np.testing.assert_array_equal(tracks_with_weather[0]["weather_data"]["10v"], [1.1])
    assert mock_ds_10u.__getitem__.return_value.sel.call_count == 1
    assert mock_ds_10v.__getitem__.return_value.sel.call_count == 1

    store.close()


def test_yield_tracks_with_weather_fills_nan_on_selection_error(make_store):
    ds = MagicMock()
    ds.data_vars = ["u10"]
    ds.__getitem__.return_value.sel.side_effect = KeyError("time")

    store = make_store({"10u": ds}, short_names=["10u"])

    def track_generator():
        yield {
            "lon": [10.0, 20.0],
            "lat": [30.0, 40.0],
            "time": [1672531200, 1675123200],
        }

    (track,) = list(store.yield_tracks_with_weather(track_generator()))

    np.testing.assert_array_equal(track["weather_data"]["10u"], [np.nan, np.nan])


def test_close_closes_datasets(make_store):
    weather_ds_map = {
        "10u": MagicMock(spec=xr.Dataset),
        "10v": MagicMock(spec=xr.Dataset),
    }
    store = make_store(weather_ds_map)

    store.close()

    for ds in weather_ds_map.values():
        ds.close.assert_called_once()

import os

import numpy as np
import pytest

from aisdb.webdata.bathymetry import Gebco, _segment_bounds

data_dir = os.environ.get(
    "AISDBDATADIR",
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "testdata",
    ),
)

y1, x1 = 48.271185186388735, -61.10595523571155
tracks_single = [
    dict(
        lon=np.array([x1]),
        lat=np.array([y1]),
        time=[0],
        dynamic=set(["time"]),
    )
]
track1K = [
    dict(
        lon=(np.random.random(1000) * 90) - 90,
        lat=(np.random.random(1000) * 90) + 0,
        time=range(1000),
        dynamic=set(["time"]),
    )
]


def test_fetch_bathygrid():
    print(f"ENV: {os.getenv('AISDBDATADIR')=}")
    print(f"checking bathymetry rasters: {data_dir=}")
    bathy = Gebco(data_dir=data_dir)
    assert bathy


def test_bathymetry_1K_pillow():
    with Gebco(data_dir=data_dir) as bathy:
        for updated in bathy.merge_tracks(track1K):
            assert "depth_metres" in updated.keys()


def test_bathymetry_single_pillow():
    with Gebco(data_dir=data_dir) as bathy:
        test = list(bathy.merge_tracks(tracks_single))
        assert "depth_metres" in test[0].keys()
        assert "depth_metres" in test[0]["dynamic"]
        print(test[0]["depth_metres"])


# regression test for the adjacent-element comparison bug: segmentation
# previously compared raster_keys[:-1] against raster_keys[:1] (the first
# element broadcast) instead of raster_keys[1:]. runs without external data.
@pytest.mark.parametrize(
    "keys,expected",
    [
        (["a"], [0, 1]),
        (["a", "a", "a"], [0, 3]),
        (["a", "b"], [0, 1, 2]),
        (["a", "a", "b", "b", "b", "c"], [0, 2, 5, 6]),
        (["a", "b", "a"], [0, 1, 2, 3]),
        (["b", "a", "a", "a"], [0, 1, 4]),
    ],
)
def test_segment_bounds(keys, expected):
    keys = np.array(keys, dtype=object)
    bounds = _segment_bounds(keys)
    assert bounds.tolist() == expected
    # every segment is one maximal run of a single repeated key
    for i in range(len(bounds) - 1):
        segment = keys[bounds[i] : bounds[i + 1]]
        assert len(set(segment)) == 1
        if i > 0:
            assert keys[bounds[i] - 1] != segment[0]

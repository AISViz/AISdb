import os
import numpy as np

from aisdb.webdata.bathymetry import Gebco, Gebco_Rasterio

data_dir = os.environ.get(
    'AISDBDATADIR',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
    ),
)

y1, x1 = 48.271185186388735, -61.10595523571155
lon1M = (np.random.random(1000000) * 90) - 90
lat1M = (np.random.random(1000000) * 90) + 0

tracks_single = [dict(
    lon=[x1],
    lat=[y1],
    time=[0],
    dynamic=set(['time']),
)]
track1K = [
    dict(
        lon=lon1M[:1000],
        lat=lat1M[:1000],
        time=range(1000),
        dynamic=set(['time']),
    )
]
track1M = [
    dict(
        lon=lon1M,
        lat=lat1M,
        time=range(len(lon1M)),
        dynamic=set(['time']),
    )
]


def test_fetch_bathygrid():
    print(f'checking bathymetry rasters: {data_dir=}')
    bathy = Gebco(data_dir=data_dir)
    assert bathy


def test_bathymetry_single_pillow():
    with Gebco(data_dir=data_dir) as bathy:
        test = list(bathy.merge_tracks(tracks_single))
        assert 'depth_metres' in test[0].keys()
        print(test[0]['depth_metres'])


def test_bathymetry_single_rasterio():
    with Gebco_Rasterio(data_dir=data_dir) as bathy:
        test = list(bathy.merge_tracks(tracks_single))
        assert 'depth_metres' in test[0].keys()
        print(test[0]['depth_metres'])


def test_timing_bathymetry_1M_pillow():
    with Gebco(data_dir=data_dir) as bathy:
        for updated in bathy.merge_tracks(track1M):
            assert 'depth_metres' in updated.keys()


def test_timing_bathymetry_1M_rasterio():
    with Gebco_Rasterio(data_dir=data_dir) as bathy:
        for updated in bathy.merge_tracks(track1M):
            assert 'depth_metres' in updated.keys()


def test_bathy_pillow_equals_rasterio():
    with Gebco_Rasterio(data_dir=data_dir) as rasterio, Gebco(
            data_dir=data_dir) as pillow:
        bathy_a = next(rasterio.merge_tracks(track1K))
        bathy_b = next(pillow.merge_tracks(track1K))
        avg_diff = np.average(
            np.abs(bathy_a['depth_metres'] - bathy_b['depth_metres']))
        print(f'{avg_diff = }')
        assert avg_diff == 0

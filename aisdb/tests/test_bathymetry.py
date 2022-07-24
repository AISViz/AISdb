import os
import numpy as np

from aisdb.webdata.bathymetry import Gebco

data_dir = os.environ.get(
    'AISDBDATADIR',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
    ),
)

y1, x1 = 48.271185186388735, -61.10595523571155


def test_fetch_bathygrid():
    print(f'checking bathymetry rasters: {data_dir=}')
    _bathy = Gebco(data_dir=data_dir)


def test_bathymetry_single():
    tracks = [dict(
        lon=[x1],
        lat=[y1],
        time=[0],
        dynamic=set(['time']),
    )]
    with Gebco(data_dir=data_dir) as bathy:
        next(bathy.merge_tracks(tracks))


def test_timing_bathymetry_1M():
    lon100k = (np.random.random(1000000) * 90) - 90
    lat100k = (np.random.random(1000000) * 90) + 0
    tracks = [
        dict(
            lon=lon100k,
            lat=lat100k,
            time=range(len(lon100k)),
            dynamic=set(['time']),
        )
    ]
    with Gebco(data_dir=data_dir) as bathy:
        for updated in bathy.merge_tracks(tracks):
            assert 'depth_metres' in updated.keys()

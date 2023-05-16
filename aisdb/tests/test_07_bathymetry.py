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
tracks_single = [
    dict(
        lon=np.array([x1]),
        lat=np.array([y1]),
        time=[0],
        dynamic=set(['time']),
    )
]
track1K = [
    dict(
        lon=(np.random.random(1000) * 90) - 90,
        lat=(np.random.random(1000) * 90) + 0,
        time=range(1000),
        dynamic=set(['time']),
    )
]


def test_fetch_bathygrid():
    print(f'ENV: {os.getenv("AISDBDATADIR")=}')
    print(f'checking bathymetry rasters: {data_dir=}')
    bathy = Gebco(data_dir=data_dir)
    assert bathy


def test_bathymetry_single_pillow():
    with Gebco(data_dir=data_dir) as bathy:
        test = list(bathy.merge_tracks(tracks_single))
        assert 'depth_metres' in test[0].keys()
        assert 'depth_metres' in test[0]['dynamic']
        print(test[0]['depth_metres'])


def test_bathymetry_1K_pillow():
    with Gebco(data_dir=data_dir) as bathy:
        for updated in bathy.merge_tracks(track1K):
            assert 'depth_metres' in updated.keys()

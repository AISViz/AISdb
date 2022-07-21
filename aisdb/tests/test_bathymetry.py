import os
import aisdb
import numpy as np

y1, x1 = 48.271185186388735, -61.10595523571155
data_dir = os.environ.get(
    'AISDBDATADIR',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
    ),
)
lon100k = (np.random.random(100000) * 90) - 90
lat100k = (np.random.random(100000) * 90) + 0


def test_fetch_bathygrid():
    print(f'fetching bathymetry rasters: {data_dir=}')
    bathy = aisdb.webdata.bathymetry.Gebco(data_dir=data_dir)


def test_bathymetry_rasterio():
    bathy = aisdb.webdata.bathymetry.Gebco(data_dir=data_dir)
    v = bathy.getdepth(x1, y1)
    print(v)


def test_timing_rasterio():
    bathy = aisdb.webdata.bathymetry.Gebco(data_dir=data_dir)
    test = [bathy.getdepth(x, y) for x, y in zip(lon100k, lat100k)]
    print(test[0:15])


def test_bathymetry_pillow():
    bathy_test = aisdb.webdata.bathymetry.Gebco(data_dir=data_dir)
    v = bathy_test.getdepth_manual(x1, y1)
    print(v)


def test_timing_pillow():
    bathy = aisdb.webdata.bathymetry.Gebco(data_dir=data_dir)
    test = [bathy.getdepth_manual(x, y) for x, y in zip(lon100k, lat100k)]
    print(test[0:15])

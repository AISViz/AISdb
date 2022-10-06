import os

import numpy as np

from aisdb.webdata.shore_dist import ShoreDist, PortDist

data_dir = os.environ.get(
        'AISDBDATADIR',
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'testdata',
            ),
        )

y1, x1 = 48.271185186388735, -61.10595523571155

tracks_short = [
        dict(
            lon=np.array([x1]),
            lat=np.array([y1]),
            time=[0],
            dynamic=set(['time']),
            )
        ]


def test_ShoreDist():
    imgpath = os.path.join(data_dir, 'distance-from-shore.tif')
    with ShoreDist(data_dir=data_dir) as sdist:
        for track in sdist.get_distance(tracks_short):
            assert 'km_from_shore' in track.keys()
            assert 'km_from_shore' in track['dynamic']


def test_PortDist():
    imgpath = os.path.join(data_dir, 'distance-from-port-v20201104.tiff')
    with PortDist(imgpath=imgpath) as portdist:
        #assert hasattr(portdist, 'get_distance')
        for track in portdist.get_distance(tracks_short):
            assert 'km_from_port' in track.keys()
            assert 'km_from_port' in track['dynamic']

import os
import numpy as np
from aisdb.denoising_encoder import InlandDenoising

y1, x1 = 44.30350650164017, -63.255341082253146
y2, x2 = 44.29083001133964, -64.45527328316969  # Point (y2, x2) is inland
y3, x3 = 44.10511509303621, -64.3325524220706

data_dir = os.environ.get("AISDBDATADIR",
                          os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "testdata", ), )

tracks_short = [dict(lon=np.array([x1, x2, x3]), lat=np.array([y1, y2, y3]), time=np.array([1, 2, 3]),
                     dynamic=set(["lon", "lat", "time"]), static=set())]


def test_inland_denoising():
    cleaned_tracklist = []
    with InlandDenoising(data_dir=data_dir) as remover:
        cleaned_tracks = remover.filter_noisy_points(tracks_short)
        cleaned_tracklist.extend(cleaned_tracks)
        assert len(cleaned_tracklist[0]['time']) == len(tracks_short[0]['time']) - 1  # ensure the inland point is removed

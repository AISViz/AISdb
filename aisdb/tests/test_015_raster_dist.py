import os

import numpy as np

from aisdb.webdata.shore_dist import ShoreDist, PortDist, CoastDist

y1, x1 = 48.271185186388735, -61.10595523571155

data_dir = os.environ.get("AISDBDATADIR",
                          os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "testdata", ), )

tracks_short = [dict(lon=np.array([x1]), lat=np.array([y1]), time=[0], dynamic=set(["time"]), )]


def test_CoastDist():
    with CoastDist(data_dir=data_dir) as coast_dist:
        for track in coast_dist.get_distance(tracks_short):
            assert "km_from_coast" in track.keys()
            assert "km_from_coast" in track["dynamic"]


def test_ShoreDist():
    with ShoreDist(data_dir=data_dir) as shore_dist:
        for track in shore_dist.get_distance(tracks_short):
            assert "km_from_shore" in track.keys()
            assert "km_from_shore" in track["dynamic"]


def test_PortDist():
    with PortDist(data_dir=data_dir) as port_dist:
        for track in port_dist.get_distance(tracks_short):
            assert "km_from_port" in track.keys()
            assert "km_from_port" in track["dynamic"]

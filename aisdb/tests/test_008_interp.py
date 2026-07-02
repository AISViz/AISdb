import os
from datetime import datetime, timedelta

import numpy as np
from pyproj import Transformer

from aisdb import track_gen, sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb.interp import interp_time, geo_interp_time, interp_cubic_spline
from aisdb.tests.create_testing_data import sample_database_file


def test_interp(tmpdir):
    dbpath = os.path.join(tmpdir, "test_interp.db")
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn(dbpath) as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(fcn=sqlfcn.crawl_dynamic_static, verbose=True)
        tracks = interp_time(
            track_gen.TrackGen(rowgen, decimate=True),
            step=timedelta(hours=0.5),
        )

        geo_tracks = geo_interp_time(
            track_gen.TrackGen(rowgen, decimate=True),
            step=timedelta(hours=0.5),
        )

        cubic_spline_tracks = interp_cubic_spline(
            track_gen.TrackGen(rowgen, decimate=True),
            step=timedelta(hours=0.5),
        )

        for track in tracks:
            assert "time" in track.keys()
            if len(track["time"]) >= 3:
                print(track)

        for track in geo_tracks:
            assert "time" in track.keys()
            if len(track["time"]) >= 3:
                print(track)

        for track in cubic_spline_tracks:
            assert "time" in track.keys()
            if len(track["time"]) >= 3:
                print(track)


def _sample_track():
    return dict(
        lon=np.array([-45.0, -44.0]),
        lat=np.array([60.0, 61.0]),
        time=np.array([0, 3600]),
        dynamic=set(["lon", "lat", "time"]),
        static=set(),
    )


def test_interp_time_follows_projected_path():
    fwd = Transformer.from_crs(4326, 3857, always_xy=True)
    back = Transformer.from_crs(3857, 4326, always_xy=True)
    track = _sample_track()
    src_x, src_y = fwd.transform(track["lon"], track["lat"])

    result = next(interp_time([track], step=timedelta(minutes=30)))
    assert np.array_equal(result["time"], np.array([0, 1800, 3600]))

    # expected midpoint is the linear midpoint in EPSG:3857, unprojected
    mid_x = (src_x[0] + src_x[1]) / 2
    mid_y = (src_y[0] + src_y[1]) / 2
    expected_mid = back.transform(mid_x, mid_y)

    assert np.allclose(result["lon"][[0, 2]], track["lon"], atol=1e-9)
    assert np.allclose(result["lat"][[0, 2]], track["lat"], atol=1e-9)
    assert abs(result["lon"][1] - expected_mid[0]) < 1e-9
    assert abs(result["lat"][1] - expected_mid[1]) < 1e-9

    # projected-path midpoint differs from naive degree-space midpoint
    naive_mid_lat = (track["lat"][0] + track["lat"][1]) / 2
    assert abs(result["lat"][1] - naive_mid_lat) > 1e-3


def test_interp_cubic_spline_follows_projected_path():
    fwd = Transformer.from_crs(4326, 3857, always_xy=True)
    back = Transformer.from_crs(3857, 4326, always_xy=True)
    track = _sample_track()
    src_x, src_y = fwd.transform(track["lon"], track["lat"])

    result = next(interp_cubic_spline([track], step=timedelta(minutes=30)))
    assert np.array_equal(result["time"], np.array([0, 1800, 3600]))

    # two points make the spline linear; midpoint matches EPSG:3857 midpoint
    mid_x = (src_x[0] + src_x[1]) / 2
    mid_y = (src_y[0] + src_y[1]) / 2
    expected_mid = back.transform(mid_x, mid_y)

    assert np.allclose(result["lon"][[0, 2]], track["lon"], atol=1e-6)
    assert np.allclose(result["lat"][[0, 2]], track["lat"], atol=1e-6)
    assert abs(result["lon"][1] - expected_mid[0]) < 1e-6
    assert abs(result["lat"][1] - expected_mid[1]) < 1e-6


def test_geo_interp_time_follows_projected_path():
    fwd = Transformer.from_crs(4326, 3857, always_xy=True)
    back = Transformer.from_crs(3857, 4326, always_xy=True)
    track = _sample_track()
    src_x, src_y = fwd.transform(track["lon"], track["lat"])

    result = next(
        geo_interp_time([track], step=timedelta(minutes=30), original_crs=4326)
    )

    mid_x = (src_x[0] + src_x[1]) / 2
    mid_y = (src_y[0] + src_y[1]) / 2
    expected_mid = back.transform(mid_x, mid_y)

    assert abs(result["lon"][1] - expected_mid[0]) < 1e-9
    assert abs(result["lat"][1] - expected_mid[1]) < 1e-9

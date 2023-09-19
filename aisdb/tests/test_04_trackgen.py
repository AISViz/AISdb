import os
from datetime import datetime, timedelta

import numpy as np

from aisdb import track_gen, sqlfcn_callbacks
from aisdb.gis import vesseltrack_3D_dist, mask_in_radius_2D
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb import encode_greatcircledistance
from aisdb.track_gen import min_speed_filter
from aisdb.tests.create_testing_data import sample_database_file


def test_TrackGen(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn(dbpath) as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.valid_mmsi,
        )
        rowgen = qry.gen_qry(verbose=True)
        tracks = track_gen.TrackGen(rowgen, decimate=True)

        for track in tracks:
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)
            assert isinstance(track['lon'], np.ndarray)
            assert isinstance(track['lat'], np.ndarray)
            assert isinstance(track['time'], np.ndarray)


def test_min_speed_filter(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen_min_speed_filter_encode.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    target_xy = [44.51204273779117, -63.47468122107318][::-1]

    with DBConn(dbpath) as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(verbose=True)
        tracks = track_gen.TrackGen(rowgen, decimate=True)
        tracks = encode_greatcircledistance(tracks, distance_threshold=250000)
        tracks = min_speed_filter(tracks, minspeed=5)
        tracks = mask_in_radius_2D(tracks, target_xy, distance_meters=100000)
        tracks = vesseltrack_3D_dist(tracks, *target_xy, 0)
        for track in tracks:
            assert 'time' in track.keys()

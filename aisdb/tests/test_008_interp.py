import os
from datetime import datetime, timedelta

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
        qry = DBQuery(dbconn=dbconn, start=start, end=end, callback=sqlfcn_callbacks.in_timerange_validmmsi, )
        rowgen = qry.gen_qry(fcn=sqlfcn.crawl_dynamic_static, verbose=True)
        tracks = interp_time(track_gen.TrackGen(rowgen, decimate=True), step=timedelta(hours=0.5), )

        geo_tracks = geo_interp_time(track_gen.TrackGen(rowgen, decimate=True), step=timedelta(hours=0.5), )

        cubic_spline_tracks = interp_cubic_spline(track_gen.TrackGen(rowgen, decimate=True), step=timedelta(hours=0.5), )

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
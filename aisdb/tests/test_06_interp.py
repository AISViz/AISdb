import os
from datetime import datetime, timedelta
import pytest

from aiosqlite.core import Connection

from aisdb import track_gen, sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery, DBQuery_async
from aisdb.track_gen import encode_greatcircledistance_async
from aisdb.tests.create_testing_data import sample_database_file
from aisdb.interp import interp_time, interp_time_async


def test_interp(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_interp.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn() as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            dbpath=dbpath,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(verbose=True)
        tracks = interp_time(
            track_gen.TrackGen(rowgen),
            step=timedelta(hours=0.5),
        )

        for track in tracks:
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)


@pytest.mark.asyncio
async def test_interp_async(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_interp_async.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    qry = DBQuery_async(
        dbpath=dbpath,
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    rowgen = qry.gen_qry(fcn=sqlfcn.crawl_dynamic_static)
    tracks = interp_time_async(
        track_gen.TrackGen_async(rowgen),
        step=timedelta(hours=0.5),
    )

    async for track in tracks:
        assert 'lon' in track.keys()
        assert 'time' in track.keys()
        if len(track['time']) >= 3:
            print(track)

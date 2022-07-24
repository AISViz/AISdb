import os
from datetime import datetime, timedelta
import pytest

from aiosqlite.core import Connection

from aisdb import track_gen, sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn, DBConn_async
from aisdb.database.dbqry import DBQuery, DBQuery_async
from aisdb.track_gen import (
    encode_greatcircledistance_async,
    min_speed_filter_async,
    split_timedelta_async,
)
from aisdb.track_gen import encode_greatcircledistance, min_speed_filter
from aisdb.tests.create_testing_data import sample_database_file

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)


def test_TrackGen(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen_encode.db')
    sample_database_file(dbpath)

    with DBConn(dbpath=dbpath) as db:
        qry = DBQuery(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(dbpath, printqry=True)
        tracks = encode_greatcircledistance(
            track_gen.TrackGen(rowgen),
            distance_threshold=250000,
        )

        for track in tracks:
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)


def test_min_speed_filter(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen_speed_filter.db')
    sample_database_file(dbpath)

    with DBConn(dbpath=dbpath) as db:
        qry = DBQuery(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(dbpath, printqry=True)
        tracks = min_speed_filter(encode_greatcircledistance(
            track_gen.TrackGen(rowgen),
            distance_threshold=250000,
        ),
                                  minspeed=5)
        for track in tracks:
            assert 'time' in track.keys()


@pytest.mark.asyncio
async def test_TrackGen_async(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen_async.db')
    sample_database_file(dbpath)

    async with DBConn_async(dbpath=dbpath) as db:
        assert isinstance(db.conn, Connection)

        qry = DBQuery_async(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(dbpath, fcn=sqlfcn.crawl_dynamic_static)
        tracks = min_speed_filter_async(
            encode_greatcircledistance_async(
                split_timedelta_async(
                    track_gen.TrackGen_async(rowgen),
                    maxdelta=timedelta(weeks=1),
                ),
                distance_threshold=250000,
            ),
            minspeed=3,
        )

        async for track in tracks:
            assert 'lon' in track.keys()
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)

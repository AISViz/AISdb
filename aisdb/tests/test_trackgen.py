import os
from datetime import datetime
import pytest

from aiosqlite.core import Connection

from aisdb import track_gen, decode_msgs, sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn, DBConn_async
from aisdb.database.dbqry import DBQuery, DBQuery_async
from aisdb.track_gen import encode_greatcircledistance_async
from aisdb.track_gen import encode_greatcircledistance, min_speed_filter
from aisdb.database.create_tables import aggregate_static_msgs

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)


def test_TrackGen(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen_encode.db')
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    with DBConn(dbpath=dbpath) as db:
        decode_msgs(
            db=db,
            filepaths=[datapath],
            dbpath=dbpath,
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )

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
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')

    with DBConn(dbpath=dbpath) as db:
        decode_msgs(
            db=db,
            filepaths=[datapath],
            dbpath=dbpath,
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )
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
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    with DBConn(dbpath=dbpath) as db1:
        decode_msgs(
            db=db1,
            filepaths=[datapath],
            dbpath=dbpath,
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )
        aggregate_static_msgs(db1, ["202111"])

    async with DBConn_async(dbpath=dbpath) as db:
        assert isinstance(db.conn, Connection)

        qry = DBQuery_async(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(dbpath, fcn=sqlfcn.crawl_dynamic_static)
        tracks = encode_greatcircledistance_async(
            track_gen.TrackGen_async(rowgen),
            distance_threshold=250000,
        )

        async for track in tracks:
            assert 'lon' in track.keys()
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)

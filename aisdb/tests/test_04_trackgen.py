import os
from datetime import datetime, timedelta
import pytest

from aisdb import track_gen, sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery, DBQuery_async
from aisdb.track_gen import (
    encode_greatcircledistance_async,
    min_speed_filter_async,
    split_timedelta_async,
)
from aisdb.track_gen import encode_greatcircledistance, min_speed_filter
from aisdb.tests.create_testing_data import sample_database_file


def test_TrackGen(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn() as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            dbpath=dbpath,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.valid_mmsi,
        )
        rowgen = qry.gen_qry(printqry=True)
        tracks = track_gen.TrackGen(rowgen)

        for track in tracks:
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)


def test_min_speed_filter(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen_min_speed_filter_encode.db')
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
        rowgen = qry.gen_qry(printqry=True)
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

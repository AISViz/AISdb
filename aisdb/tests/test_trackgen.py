import os
from datetime import datetime
import pytest

from aisdb import track_gen, decode_msgs, DBQuery, sqlfcn, sqlfcn_callbacks
from aisdb.track_gen import encode_greatcircledistance_async
from aisdb.track_gen import encode_greatcircledistance, min_speed_filter

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)


def test_TrackGen(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    decode_msgs(
        filepaths=[datapath],
        dbpath=dbpath,
        source='TESTING',
        vacuum=False,
        skip_checksum=True,
    )

    qry = DBQuery(
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
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    decode_msgs(
        filepaths=[datapath],
        dbpath=dbpath,
        source='TESTING',
        vacuum=False,
        skip_checksum=True,
    )
    qry = DBQuery(
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
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    decode_msgs(
        filepaths=[datapath],
        dbpath=dbpath,
        source='TESTING',
        vacuum=False,
        skip_checksum=True,
    )

    qry = DBQuery(
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    rowgen = qry.async_qry(dbpath, fcn=sqlfcn.crawl_dynamic_static)
    tracks = encode_greatcircledistance_async(
        track_gen.TrackGen_async(rowgen),
        distance_threshold=250000,
    )

    async for track in tracks:
        assert 'lon' in track.keys()
        assert 'time' in track.keys()
        if len(track['time']) >= 3:
            print(track)

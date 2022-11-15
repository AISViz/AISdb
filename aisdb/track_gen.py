''' generation, segmentation, and filtering of vessel trajectories '''

from functools import reduce
from datetime import timedelta
import sqlite3
import types

import numpy as np
import warnings
import orjson

from aisdb.aisdb import simplify_linestring_idx, encoder_score_fcn
from aisdb.gis import delta_knots, delta_meters
from aisdb.proc_util import _segment_rng

staticcols = set([
    'mmsi', 'vessel_name', 'ship_type', 'ship_type_txt', 'dim_bow',
    'dim_stern', 'dim_port', 'dim_star', 'imo'
])


def _segment_longitude(track, tolerance=300):
    ''' segment track vectors where difference in longitude exceeds 300 degrees
    '''

    if len(track['time']) == 1:
        yield track
        return

    diff = np.nonzero(
        np.abs(track['lon'][1:] - track['lon'][:-1]) > tolerance)[0] + 1

    if diff.size == 0:
        assert 'time' in track.keys()
        yield track
        return

    segments_idx = reduce(np.append, ([0], diff, [track['time'].size]))
    for i in range(segments_idx.size - 1):
        tracksplit = dict(
            **{k: track[k]
               for k in track['static']},
            **{
                k: track[k][segments_idx[i]:segments_idx[i + 1]]
                for k in track['dynamic']
            },
            static=track['static'],
            dynamic=track['dynamic'],
        )
        assert 'time' in tracksplit.keys()
        yield tracksplit


_statcols = set([
    'mmsi', 'vessel_name', 'ship_type', 'ship_type_txt', 'dim_bow',
    'dim_stern', 'dim_port', 'dim_star', 'imo', 'coarse_type_txt'
])


def _yieldsegments(rows, staticcols, dynamiccols, decimate=0.0001):
    if decimate is True:
        decimate = 0.0001
    lon = np.array([r['longitude'] for r in rows], dtype=float)
    lat = np.array([r['latitude'] for r in rows], dtype=float)
    time = np.array([r['time'] for r in rows], dtype=np.uint32)
    if decimate is not False:
        idx = simplify_linestring_idx(lon, lat, precision=decimate)
    else:
        idx = np.array(range(len(lon)))
    trackdict = dict(
        **{col: rows[0][col]
           for col in staticcols},
        lon=lon[idx].astype(np.float32),
        lat=lat[idx].astype(np.float32),
        time=time[idx],
        sog=np.array([r['sog'] for r in rows], dtype=np.float32)[idx],
        cog=np.array([r['cog'] for r in rows], dtype=np.uint32)[idx],
        static=staticcols,
        dynamic=dynamiccols,
    )
    assert 'time' in trackdict.keys()

    for segment in _segment_longitude(trackdict):
        for key in segment['dynamic']:
            assert len(segment[key]) == len(segment['time'])
        yield segment


def TrackGen(rowgen: iter, decimate: float = 0.0001) -> dict:
    ''' generator converting sets of rows sorted by MMSI to a
        dictionary containing track column vectors.
        each row contains columns from database: mmsi time lon lat name ...
        rows must be sorted by first by mmsi, then time

        args:
            rowgen (aisdb.database.dbqry.DBQuery.gen_qry())
                DBQuery rows generator. Yields rows returned
                by a database query
            decimate (bool)
                if True, linear curve decimation will be applied to reduce
                the number of unnecessary datapoints

        yields:
            dictionary containing track column vectors.
            static data (e.g. mmsi, name, geometry) will be stored as
            scalar values

        >>> import os
        >>> from datetime import datetime
        >>> from aisdb import DBConn, DBQuery, TrackGen, decode_msgs
        >>> from aisdb.database import sqlfcn_callbacks

        >>> # create example database file
        >>> dbpath = './testdata/test.db'
        >>> filepaths = ['aisdb/tests/test_data_20210701.csv',
        ...              'aisdb/tests/test_data_20211101.nm4']

        >>> with DBConn() as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn, dbpath=dbpath,
        ...     source='TESTING')
        ...     q = DBQuery(callback=sqlfcn_callbacks.in_timerange_validmmsi,
        ...             dbconn=dbconn,
        ...             dbpath=dbpath,
        ...             start=datetime(2021, 7, 1),
        ...             end=datetime(2021, 7, 7))
        ...     rowgen = q.gen_qry()
        ...     for track in TrackGen(rowgen):
        ...         print(track['mmsi'], track['lon'], track['lat'], track['time'])
        ...         break
        204242000 [-8.931666] [41.45] [1625176725]
        >>> os.remove(dbpath)
    '''
    firstrow = True
    assert isinstance(rowgen, types.GeneratorType)
    for rows in rowgen:
        assert not (rows is None or len(rows) == 0), 'rows cannot be empty'
        assert isinstance(rows[0], (sqlite3.Row, dict))
        if firstrow:
            staticcols = set(rows[0].keys()) & _statcols
            dynamiccols = set(rows[0].keys()) ^ staticcols
            dynamiccols = dynamiccols.difference(set(['longitude',
                                                      'latitude']))
            dynamiccols = dynamiccols.union(set(['lon', 'lat', 'time']))
            firstrow = False
        for track in _yieldsegments(rows, staticcols, dynamiccols, decimate):
            yield track


async def TrackGen_async(rowgen: iter, decimate: float = 0.0001) -> dict:
    ''' generator converting sets of rows sorted by MMSI to a
        dictionary containing track column vectors.
        each row contains columns from database: mmsi time lon lat name ...
        rows must be sorted by first by mmsi, then time

        args:
            rowgen (aisdb.database.dbqry.DBQuery.gen_qry())
                DBQuery rows generator. Yields rows returned
                by a database query
            decimate (bool or int)
                if True, linear curve decimation will be applied to reduce
                the number of unnecessary datapoints with a default precision
                of 0.0001.
                If Float, this value will be used as the level of precision.


        yields:
            dictionary containing track column vectors.
            static data (e.g. mmsi, name, geometry) will be stored as
            scalar values

        >>> import os
        >>> import asyncio
        >>> from datetime import datetime
        >>> from aisdb import DBConn, DBQuery_async, TrackGen_async, decode_msgs
        >>> from aisdb.database import sqlfcn_callbacks

        >>> # create example database file
        >>> dbpath = './testdata/test.db'
        >>> filepaths = ['aisdb/tests/test_data_20210701.csv',
        ...              'aisdb/tests/test_data_20211101.nm4']

        >>> with DBConn() as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn, dbpath=dbpath,
        ...     source='TESTING')
        >>> async def get_tracks():
        ...     qry = DBQuery_async(
        ...             dbpath=dbpath,
        ...             start=datetime(2021, 7, 1),
        ...             end=datetime(2021, 7, 7),
        ...             callback=sqlfcn_callbacks.in_timerange_validmmsi)
        ...     tracks = TrackGen_async(qry.gen_qry())
        ...     async for track in tracks:
        ...         print(track['mmsi'], track['lon'], track['lat'], track['time'])
        ...         break
        ...     # since loop is exited early, need to clean up resources
        ...     await qry.dbconn.close()  # close async database connection
        ...     await tracks.aclose()  # close async event loop
        >>> asyncio.run(get_tracks())
        204242000 [-8.931666] [41.45] [1625176725]
        >>> os.remove(dbpath)
    '''
    firstrow = True
    async for rows in rowgen:
        assert not (rows is None or len(rows) == 0), 'rows cannot be empty'
        assert isinstance(rows[0], sqlite3.Row)
        if firstrow:
            staticcols = set(rows[0].keys()) & _statcols
            dynamiccols = set(rows[0].keys()) ^ staticcols
            dynamiccols = dynamiccols.difference(set(['longitude',
                                                      'latitude']))
            dynamiccols = dynamiccols.union(set(['lon', 'lat', 'time']))
            firstrow = False
        for track in _yieldsegments(rows, staticcols, dynamiccols, decimate):
            yield track


def split_timedelta(tracks, maxdelta=timedelta(weeks=2)):
    ''' partitions tracks where delta time exceeds maxdelta

        args:
            tracks (aisdb.track_gen.TrackGen)
                track vectors generator
            maxdelta (datetime.timedelta)
                threshold at which tracks should be
                partitioned
    '''
    for track in tracks:
        for rng in _segment_rng(track, maxdelta):
            assert len(rng) > 0
            yield dict(
                **{k: track[k]
                   for k in track['static']},
                **{
                    k: np.array(track[k], dtype=object)[rng]
                    for k in track['dynamic']
                },
                static=track['static'],
                dynamic=track['dynamic'],
            )


async def split_timedelta_async(tracks, maxdelta=timedelta(weeks=2)):
    ''' partitions tracks where delta time exceeds maxdelta

        args:
            tracks (aisdb.track_gen.TrackGen)
                track vectors generator
            maxdelta (datetime.timedelta)
                threshold at which tracks should be
                partitioned
    '''
    async for track in tracks:
        for rng in _segment_rng(track, maxdelta):
            assert len(rng) > 0
            yield dict(
                **{k: track[k]
                   for k in track['static']},
                **{k: track[k][rng]
                   for k in track['dynamic']},
                static=track['static'],
                dynamic=track['dynamic'],
            )


def _score_idx(scores):
    ''' Returns indices of score array where value at index is equal to the
        highest score. In tie cases, the last index will be selected
    '''
    assert len(scores) > 0
    return np.where(scores == np.max(scores))[0][-1]


def _segments_idx(track, distance_threshold, speed_threshold, **_):
    segments_idx1 = reduce(
        np.append, ([0], np.where(delta_knots(track) > speed_threshold)[0] + 1,
                    [track['time'].size]))
    segments_idx2 = reduce(
        np.append,
        ([0], np.where(delta_meters(track) > distance_threshold)[0] + 1,
         [track['time'].size]))

    return reduce(np.union1d, (segments_idx1, segments_idx2))


def _scoresarray(track, *, pathways, i, segments_idx, distance_threshold,
                 speed_threshold, minscore):
    scores = np.array([
        encoder_score_fcn(
            x1=pathway['lon'][-1],
            y1=pathway['lat'][-1],
            t1=pathway['time'][-1],
            x2=track['lon'][segments_idx[i]],
            y2=track['lat'][segments_idx[i]],
            t2=track['time'][segments_idx[i]],
            dist_thresh=distance_threshold,
            speed_thresh=speed_threshold,
        ) for pathway in pathways
    ],
                      dtype=np.float16)
    highscore = (scores[np.where(
        scores == np.max(scores))[0][0]] if scores.size > 0 else minscore)
    return scores, highscore


def _append_highscore(track, *, highscoreidx, pathways, i, segments_idx):
    return dict(
        **{k: track[k]
           for k in track['static']},
        **{
            k: np.append(pathways[highscoreidx][k],
                         track[k][segments_idx[i]:segments_idx[i + 1]])
            for k in track['dynamic']
        },
        static=track['static'],
        dynamic=track['dynamic'],
    )


def _split_pathway(track, *, i, segments_idx):
    path = dict(
        **{k: track[k]
           for k in track['static']},
        **{
            k: track[k][segments_idx[i]:segments_idx[i + 1]]
            for k in track['dynamic']
        },
        static=track['static'],
        dynamic=track['dynamic'],
    )
    return path


def _score_encode(track, distance_threshold, speed_threshold, minscore):
    assert 'time' in track.keys()
    assert len(track['time']) > 0
    params = dict(distance_threshold=distance_threshold,
                  speed_threshold=speed_threshold,
                  minscore=minscore)
    segments_idx = _segments_idx(track, **params)
    pathways = []
    warned = False
    for i in range(segments_idx.size - 1):
        if len(pathways) == 0:
            path = _split_pathway(track, i=i, segments_idx=segments_idx)
            assert path is not None
            pathways.append(path)
            continue
        elif not warned and len(pathways) > 100:
            warnings.warn(
                f'excessive number of pathways! mmsi={track["mmsi"]}')
            warned = True
        assert len(track['time']) > 0, f'{track=}'

        scores, highscore = _scoresarray(track,
                                         pathways=pathways,
                                         i=i,
                                         segments_idx=segments_idx,
                                         **params)
        assert len(scores) > 0, f'{track}'
        if (highscore >= minscore):
            highscoreidx = _score_idx(scores)
            pathways[highscoreidx] = _append_highscore(
                track,
                highscoreidx=highscoreidx,
                pathways=pathways,
                i=i,
                segments_idx=segments_idx)
        else:
            path = _split_pathway(track, i=i, segments_idx=segments_idx)
            assert path is not None
            pathways.append(path.copy())

    for pathway, label in zip(pathways, range(len(pathways))):
        pathway['label'] = label
        pathway['static'] = set(pathway['static']).union({'label'})
        assert 'label' in pathway.keys()
        assert 'time' in pathway.keys(), f'{pathway=}'
        yield pathway


def encode_greatcircledistance(
    tracks,
    *,
    distance_threshold,
    speed_threshold=50,
    minscore=1e-6,
):
    ''' partitions tracks where delta speeds exceed speed_threshold or
        delta_meters exceeds distance_threshold.
        concatenates track segments with the highest likelihood of being
        sequential, as encoded by a distance/time score function

        args:
            tracks (aisdb.track_gen.TrackGen)
                track vectors generator
            distance_threshold (int)
                distance in meters that will be used as a
                speed score numerator
            time_threshold (datetime.timedelta)
            minscore (float)
                minimum score threshold at which to allow track
                segments to be linked

        >>> import os
        >>> from datetime import datetime, timedelta
        >>> from aisdb import DBConn, DBQuery, TrackGen
        >>> from aisdb import decode_msgs, encode_greatcircledistance, sqlfcn_callbacks

        >>> # create example database file
        >>> dbpath = './testdata/test.db'
        >>> filepaths = ['aisdb/tests/test_data_20210701.csv',
        ...              'aisdb/tests/test_data_20211101.nm4']
        >>> with DBConn() as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn,
        ...     dbpath=dbpath, source='TESTING')

        >>> with DBConn() as dbconn:
        ...     q = DBQuery(callback=sqlfcn_callbacks.in_timerange_validmmsi,
        ...             dbconn=dbconn,
        ...             dbpath=dbpath,
        ...             start=datetime(2021, 7, 1),
        ...             end=datetime(2021, 7, 7))
        ...     tracks = TrackGen(q.gen_qry())
        ...     for track in encode_greatcircledistance(
        ...             tracks,
        ...             distance_threshold=250000,  # metres
        ...             speed_threshold=50,  # knots
        ...             minscore=0):
        ...         print(track['mmsi'])
        ...         print(track['lon'], track['lat'])
        ...         break
        204242000
        [-8.931666] [41.45]
        >>> os.remove(dbpath)

    '''
    for track in tracks:
        assert isinstance(track, dict), f'got {type(track)} {track}'
        for path in _score_encode(track, distance_threshold, speed_threshold,
                                  minscore):
            yield path


async def encode_greatcircledistance_async(tracks,
                                           *,
                                           distance_threshold,
                                           speed_threshold=50,
                                           minscore=1e-6):
    ''' args:
            tracks (:func:`aisdb.track_gen.TrackGen_async`)
                tracks generator return by TrackGen_async

        yields:
            dictionary containing track column vectors.
            static data (e.g. mmsi, name, geometry) will be stored as
            scalar values

        >>> import os
        >>> import asyncio
        >>> from datetime import datetime
        >>> from aisdb import DBConn, DBQuery_async, TrackGen_async, decode_msgs
        >>> from aisdb.database import sqlfcn_callbacks

        >>> # create example database file
        >>> dbpath = './testdata/test.db'
        >>> filepaths = ['aisdb/tests/test_data_20210701.csv',
        ...              'aisdb/tests/test_data_20211101.nm4']

        >>> with DBConn() as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn, dbpath=dbpath,
        ...     source='TESTING')
        >>> async def get_tracks():
        ...     qry = DBQuery_async(
        ...             dbpath=dbpath,
        ...             start=datetime(2021, 7, 1),
        ...             end=datetime(2021, 7, 7),
        ...             callback=sqlfcn_callbacks.in_timerange_validmmsi)
        ...     tracks = encode_greatcircledistance_async(TrackGen_async(qry.gen_qry()), distance_threshold=200000)
        ...     async for track in tracks:
        ...         print(track['mmsi'], track['lon'], track['lat'], track['time'])
        ...         break
        ...     # since loop is exited early, need to clean up resources
        ...     await qry.dbconn.close()  # close async database connection
        ...     await tracks.aclose()  # close async event loop
        >>> asyncio.run(get_tracks())
        204242000 [-8.931666] [41.45] [1625176725]
        >>> os.remove(dbpath)
    '''
    async for track in tracks:
        for path in _score_encode(
                track,
                distance_threshold,
                speed_threshold,
                minscore,
        ):
            yield path


def fence_tracks(tracks, domain):
    ''' compute points-in-polygons for vessel positions within domain polygons

        yields track dictionaries

        Also see zone_mask()
    '''
    for track in tracks:
        assert isinstance(track, dict)
        if 'in_zone' not in track.keys():
            track['in_zone'] = np.array(
                [
                    domain.point_in_polygon(x, y)
                    for x, y in zip(track['lon'], track['lat'])
                ],
                dtype=object,
            )
            track['dynamic'] = set(track['dynamic']).union(set(['in_zone']))
        yield track


def zone_mask(tracks, domain):
    ''' compute points-in-polygons for track positions, and filter results to
        positions within domain.

        yields track dictionaries.

        also see fence_tracks()
    '''
    for track in fence_tracks(tracks, domain):
        mask = track['in_zone'] != 'Z0'
        yield dict(
            **{k: track[k]
               for k in track['static']},
            **{k: track[k][mask]
               for k in track['dynamic']},
            static=track['static'],
            dynamic=track['dynamic'],
        )


def min_speed_filter(tracks, minspeed):
    for track in tracks:
        if len(track['time']) == 1:
            yield track
            continue
        deltas = delta_knots(track)
        deltas = np.append(deltas, [deltas[-1]])
        mask = deltas >= minspeed
        yield dict(
            **{k: track[k]
               for k in track['static']},
            **{k: track[k][mask]
               for k in track['dynamic']},
            static=track['static'],
            dynamic=track['dynamic'],
        )


async def min_speed_filter_async(tracks, minspeed):
    async for track in tracks:
        if len(track['time']) == 1:
            yield track
            continue
        deltas = delta_knots(track)
        deltas = np.append(deltas, [deltas[-1]])
        mask = deltas >= minspeed
        yield dict(
            **{k: track[k]
               for k in track['static']},
            **{k: track[k][mask]
               for k in track['dynamic']},
            static=track['static'],
            dynamic=track['dynamic'],
        )


def serialize_tracks(tracks):
    for track in tracks:
        track['static'] = tuple(track['static'])
        track['dynamic'] = tuple(track['dynamic'])
        if 'marinetraffic_info' in track.keys():  # pragma: no cover
            track['marinetraffic_info'] = dict(track['marinetraffic_info'])
        yield orjson.dumps(track, option=orjson.OPT_SERIALIZE_NUMPY)


def _deser(track_serialized):
    track = orjson.loads(track_serialized)
    for key in track['dynamic']:
        track[key] = np.array(track[key])
    return track


def deserialize_tracks(tracks):
    for track in tracks:
        yield _deser(track)

''' generation, segmentation, and filtering of vessel trajectories '''

from functools import reduce
from datetime import timedelta
import sqlite3

import numpy as np

import aisdb
from .gis import delta_knots, delta_meters
from .proc_util import _segment_rng

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


def _yieldsegments(rows, staticcols, dynamiccols):
    lon = np.array([r['longitude'] for r in rows], dtype=float)
    lat = np.array([r['latitude'] for r in rows], dtype=float)
    time = np.array([r['time'] for r in rows], dtype=np.uint32)
    idx = aisdb.simplify_linestring_idx(lon, lat, precision=0.001)
    trackdict = dict(
        **{col: rows[0][col]
           for col in staticcols},
        lon=lon[idx].astype(np.float32),
        lat=lat[idx].astype(np.float32),
        time=time[idx],
        sog=np.array([r['sog'] for r in rows], dtype=np.float16),
        cog=np.array([r['cog'] for r in rows], dtype=np.uint16),
        static=staticcols,
        dynamic=dynamiccols,
    )
    assert 'time' in trackdict.keys()

    for segment in _segment_longitude(trackdict):
        yield segment


def TrackGen(rowgen: iter) -> dict:
    ''' generator converting sets of rows sorted by MMSI to a
        dictionary containing track column vectors.
        each row contains columns from database: mmsi time lon lat name ...
        rows must be sorted by first by mmsi, then time

        args:
            rowgen (aisdb.database.dbqry.DBQuery.gen_qry())
                DBQuery rows generator. Yields rows returned
                by a database query

        yields:
            dictionary containing track column vectors.
            static data (e.g. mmsi, name, geometry) will be stored as
            scalar values

        >>> from datetime import datetime
        >>> from aisdb import DBQuery, sqlfcn_callbacks, TrackGen

        >>> dbpath = '~/ais/ais.db'
        >>> q = DBQuery(callback=sqlfcn_callbacks.in_timerange_validmmsi,
        ...             start=datetime(2022, 1, 1),
        ...             end=datetime(2022, 1, 7))
        >>> rowgen = q.gen_qry()

        >>> print(f'iterating over rows returned from {dbpath}')
        >>> for track in TrackGen(rowgen):
        ...     print(track['mmsi'])
        ...     print(f'messages in track segment: {track["time"].size}')
        ...     print(f'keys: {track.keys()}')
    '''
    firstrow = True
    for rows in rowgen:
        assert not (rows is None or len(rows) == 0), 'rows cannot be empty'
        assert isinstance(rows[0], sqlite3.Row)
        if firstrow:
            staticcols = set(rows[0].keys()) & _statcols
            dynamiccols = set(rows[0].keys()) ^ staticcols
            dynamiccols = dynamiccols.difference(set(['longitude',
                                                      'latitude']))
            dynamiccols = dynamiccols.union(set(['lon', 'lat', 'time']))
            firstrow = False
        for track in _yieldsegments(rows, staticcols, dynamiccols):
            yield track


async def TrackGen_async(rowgen: iter) -> dict:
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
        for track in _yieldsegments(rows, staticcols, dynamiccols):
            yield track


TrackGen_async.__doc__ = TrackGen.__doc__


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
                **{k: track[k][rng]
                   for k in track['dynamic']},
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


def _score_fcn(xy1, xy2, t1, t2, *, speed_threshold, distance_threshold):
    ''' Assigns a score for likelihood of two points being part of a sequential
        vessel trajectory. A hard cutoff will be applied at distance_threshold,
        after which all scores will be set to -1.

        args:
            xy1 (tuple)
                Float values containing (longitude, latitude) for the first
                coordinate location
            xy2 (tuple)
                Float values containing (longitude, latitude) for the second
                coordinate location
            t1 (float)
                Timestamp for coordinate pair xy1 in epoch seconds
            t2 (float)
                Timestamp for coordinate pair xy2 in epoch seconds
            speed_threshold (float)
                Tracks will be segmented between points where computed
                speed values exceed this threshold. Segmented tracks will
                be scored for reconnection. Measured in knots
            distance_threshold (float)
                Used as a numerator when determining score; this value
                is divided by the distance between xy1 and xy2.
                If the distance between xy1 and xy2 exceeds this value,
                the score will be set to -1. Measured in meters
    '''
    # great circle distance between coordinate pairs (meters)
    dm = max(aisdb.haversine(*xy1, *xy2), 1.)

    # elapsed time between coordinate pair timestamps (seconds)
    assert t2 >= t1
    dt = max(abs(t2 - t1), 10.)
    assert dt < 60 * 60 * 24 * 7 * 52 * 1, f'{dt=}'

    # computed speed between coordinate pairs (knots)
    ds = (dm / dt) * 1.9438444924406

    if ds < speed_threshold and dm < distance_threshold * 2:
        #if ds < speed_threshold:
        #score = ((distance_threshold / dm) / dt)
        score = distance_threshold / ds
        return score
    else:
        return -1


'''

# take average of most recent 2 scores
scores = (
    np.array([score_fcn(
            xy1=(track['lon'][segments_idx[i]], track['lat'][segments_idx[i]]),
            xy2=(pathway['lon'][
                    (idx1:=min(2, pathway['time'].size-1)*-1)
                ], pathway['lat'][idx1]),
            t1=track['time'][segments_idx[i]],
            t2=pathway['time'][idx1],
        ) for pathway in pathways ], dtype=np.float16)
    +
    np.array([score_fcn(
            xy1=(track['lon'][
                    (idx2:=segments_idx[min(i+1, segments_idx.size-1)])
                ], track['lat'][segments_idx[i]+1]),
            xy2=(pathway['lon'][-1], pathway['lat'][-1]),
            t1=track['time'][idx2],
            t2=pathway['time'][-1],
        ) for pathway in pathways ], dtype=np.float16)
    ) / 2
'''


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
        _score_fcn(
            xy1=(pathway['lon'][-1], pathway['lat'][-1]),
            xy2=(track['lon'][segments_idx[i]], track['lat'][segments_idx[i]]),
            t1=pathway['time'][-1],
            t2=track['time'][segments_idx[i]],
            distance_threshold=distance_threshold,
            speed_threshold=speed_threshold,
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


'''
def _pop_pathways(highscoreidx, pathways, n):
    pathways[highscoreidx]['label'] = n
    pathways[highscoreidx]['static'] = set(
        pathways[highscoreidx]['static']).union({'label'})
    path = pathways.pop(highscoreidx)
    assert 'time' in path.keys()
'''


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
    for i in range(segments_idx.size - 1):
        if len(pathways) == 0:
            path = _split_pathway(track, i=i, segments_idx=segments_idx)
            assert path is not None
            pathways.append(path)
            continue
        elif len(pathways) > 100:
            print(f'excessive number of pathways! mmsi={track["mmsi"]}')
            # yield pathways.pop(0)
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

        >>> from datetime import datetime, timedelta
        >>> from aisdb import dbpath, DBQuery, sqlfcn_callbacks
        >>> from aisdb import TrackGen, encode_greatcircledistance

        >>> q = DBQuery(callback=sqlfcn_callbacks.in_timerange_validmmsi,
        ...             start=datetime(2022, 1, 1),
        ...             end=datetime(2022, 1, 7))
        >>> rowgen = q.gen_qry()

        >>> for track in encode_greatcircledistance(
        ...         TrackGen(rowgen),
        ...         distance_threshold=250000, # metres
        ...         time_threshold=timedelta(hours=24),
        ...         minscore=0):
        ...     print(track['mmsi'])
        ...     print(f'messages in track segment: {track["time"].size}')
        ...     print(f'keys: {track.keys()}')
    '''
    for track in tracks:
        for path in _score_encode(track, distance_threshold, speed_threshold,
                                  minscore):
            yield path


async def encode_greatcircledistance_async(
    tracks,
    *,
    distance_threshold,
    speed_threshold=50,
    minscore=1e-6,
):
    async for track in tracks:
        for path in _score_encode(track, distance_threshold, speed_threshold,
                                  minscore):
            yield path


encode_greatcircledistance_async.__doc__ = encode_greatcircledistance.__doc__


def max_tracklength(tracks, max_length=100000):
    ''' applies a maximum track length to track vectors.
        can be used to avoid excess memory consumption

        args:
            tracks: generator
                yields track dictionaries
            max_track_length: int
                tracks exceeding this number of datapoints will be segmented

        yields track dictionaries
    '''

    for track in tracks:
        while (track['time'].size > max_length):
            yield dict(
                **{k: track[k]
                   for k in track['static']},
                **{k: track[k][:max_length]
                   for k in track['dynamic']},
                static=track['static'],
                dynamic=set(track['dynamic']),
            )
            track = dict(
                **{k: track[k]
                   for k in track['static']},
                **{k: track[k][max_length:]
                   for k in track['dynamic']},
                static=track['static'],
                dynamic=set(track['dynamic']),
            )
        yield track


def concat_realisticspeed(tracks, knots_threshold=50):
    ''' if two consecutive tracks are within a realistic speed threshold, they
        will be concatenated
    '''
    segment = next(tracks)
    for track in tracks:
        deltas = {
            'time': np.append(segment['time'][-1], track['time'][0]),
            'lon': np.append(segment['lon'][-1], track['lon'][0]),
            'lat': np.append(segment['lat'][-1], track['lat'][0]),
        }
        if segment['mmsi'] == track['mmsi'] and delta_knots(
                deltas, range(2))[0] < knots_threshold:
            segment = dict(
                **{k: segment[k]
                   for k in segment['static']},
                **{
                    k: np.append(segment[k], track[k])
                    for k in track['dynamic']
                },
                static=track['static'],
                dynamic=set(track['dynamic']),
            )
        else:
            yield segment
            segment = track
    yield segment


def fence_tracks(tracks, domain):
    ''' compute points-in-polygons for vessel positions within domain polygons

        yields track dictionaries

        Also see zone_mask()
    '''
    for track in tracks:
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

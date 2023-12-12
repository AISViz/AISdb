''' generation, segmentation, and filtering of vessel trajectories '''

from functools import reduce
from datetime import timedelta
import sqlite3
import types
import warnings
import numpy as np

from aisdb.aisdb import simplify_linestring_idx
from aisdb.gis import delta_knots
from aisdb.proc_util import _segment_rng
from aisdb import Domain

staticcols = set([
    'mmsi', 'vessel_name', 'ship_type', 'ship_type_txt', 'dim_bow',
    'dim_stern', 'dim_port', 'dim_star', 'imo', 'draught'
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


class EmptyRowsException(Exception):
    pass


def TrackGen(rowgen: iter, decimate: False) -> dict:
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
        >>> import numpy as np
        >>> from datetime import datetime
        >>> from aisdb import SQLiteDBConn, DBQuery, TrackGen, decode_msgs
        >>> from aisdb.database import sqlfcn_callbacks
        >>> # create example database file
        >>> dbpath = 'track_gen_test.db'
        >>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv',
        ...              'aisdb/tests/testdata/test_data_20211101.nm4']
        >>> with SQLiteDBConn(dbpath) as dbconn:
        ...     decode_msgs(filepaths, dbconn=dbconn, source='TESTING', verbose=False)
        ...     q = DBQuery(callback=sqlfcn_callbacks.in_timerange_validmmsi,
        ...                 dbconn=dbconn,
        ...                 start=datetime(2021, 7, 1),
        ...                 end=datetime(2021, 7, 7))
        ...     rowgen = q.gen_qry()
        ...     for track in TrackGen(rowgen, decimate=True):
        ...         result = (track['mmsi'], track['lon'], track['lat'], track['time'])
        ...         assert result == (204242000, np.array([-8.931666], dtype=np.float32),
        ...                           np.array([41.45], dtype=np.float32), np.array([1625176725], dtype=np.uint32))
        ...         break

    '''
    '''
        >>> os.remove(dbpath)
    '''
    firstrow = True
    assert isinstance(rowgen, types.GeneratorType)
    for rows in rowgen:
        if (rows is None or len(rows) == 0):
            warnings.warn('No results for query!')
            return dict()
            # raise EmptyRowsException('rows cannot be empty')
        assert isinstance(
            rows[0], (sqlite3.Row, dict)), f'unknown row type: {type(rows[0])}'
        if firstrow:
            keys = set(rows[0].keys())
            static = keys.intersection(set(staticcols))
            dynamiccols = keys ^ static
            dynamiccols = dynamiccols.difference(set(['longitude',
                                                      'latitude']))
            dynamiccols = dynamiccols.union(set(['lon', 'lat']))
            firstrow = False
        for track in _yieldsegments(rows, static, dynamiccols, decimate):
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
                    k: np.array(track[k], dtype=type(track[k][0]))[rng]
                    for k in track['dynamic']
                },
                static=track['static'],
                dynamic=track['dynamic'],
            )


def fence_tracks(tracks, domain):
    ''' compute points-in-polygons for vessel positions within domain polygons

        yields track dictionaries

        Also see zone_mask()
    '''
    assert isinstance(domain, Domain), 'Not a domain object'

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

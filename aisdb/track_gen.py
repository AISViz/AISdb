''' generation, segmentation, and filtering of vessel trajectories '''

import sqlite3
import types
import warnings
from datetime import timedelta
from functools import reduce

import numpy as np
from aisdb.aisdb import simplify_linestring_idx

from aisdb import Domain
from aisdb.gis import delta_knots
from aisdb.proc_util import _segment_rng, _segment_rng_all

staticcols = set([
    'mmsi', 'vessel_name', 'ship_type', 'ship_type_txt', 'dim_bow', 'maneuver',
    'dim_stern', 'dim_port', 'dim_star', 'imo', 'draught',
    'destination', 'eta_month', 'eta_day', 'eta_hour', 'eta_minute',
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
        dynamic=dynamiccols,
        static=staticcols,
        time=time[idx],
        lon=lon[idx].astype(np.float32),
        lat=lat[idx].astype(np.float32),
        cog=np.array([r['cog'] for r in rows], dtype=np.uint32)[idx],
        sog=np.array([r['sog'] for r in rows], dtype=np.float32)[idx],
        heading=np.array([r['heading'] for r in rows], dtype=np.float32)[idx],
        rot=np.array([r['rot'] for r in rows], dtype=np.float32)[idx],
        utc_second=np.array([r['utc_second'] for r in rows], dtype=np.uint32)[idx],
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
    mmsi_count = {}  # Dictionary to keep track of MMSI indices

    for track in tracks:
        for rng in _segment_rng(track, maxdelta):
            assert len(rng) > 0

            # Create the segmented track dictionary
            segmented_track = dict(
                **{k: track[k] for k in track['static']},
                **{
                    k: np.array(track[k], dtype=type(track[k][0]))[rng]
                    for k in track['dynamic']
                },
                static=track['static'],
                dynamic=track['dynamic'],
            )

            # Handle MMSI indexing after segmentation
            mmsi_value = segmented_track.get("mmsi")
            if mmsi_value:
                if mmsi_value not in mmsi_count:
                    mmsi_count[mmsi_value] = 0
                else:
                    mmsi_count[mmsi_value] += 1

                # Modify the mmsi value to attach an index
                # segmented_track["mmsi"] = f"{mmsi_value}-{mmsi_count[mmsi_value]}"
                segmented_track["idx"] = mmsi_count[mmsi_value]

            # Yield the segmented track with modified mmsi
            yield segmented_track


def split_tracks(tracks, max_distance=25000, max_time=timedelta(hours=24),
                 max_speed=50, min_speed=0.2, min_segment_length=15, min_direction_change=45):
    """
    Segments AIS tracks based on multiple criteria such as course changes, speed, distance, and time gaps.
    Args:
        tracks (aisdb.track_gen.TrackGen): track vectors generator
        max_distance (float): Maximum allowable distance (meters) between points in a segment.
        max_time (timedelta): Maximum allowable time difference between points in a segment.
        max_speed (float): Maximum allowable speed (knots).
        min_speed (float): Minimum allowable speed (knots).
        min_segment_length (int): Minimum number of points required in a segment.
        min_direction_change (float): Minimum course change (degrees) to start a new segment.
    """
    mmsi_count = {}  # Dictionary to keep track of MMSI indices
    for track in tracks:
        for rng in _segment_rng_all(track, max_distance, max_time, max_speed, min_speed, min_segment_length, min_direction_change):
            assert len(rng) > 0

            # Create the segmented track dictionary
            segmented_track = dict(
                **{k: track[k] for k in track['static']},
                **{
                    k: np.array(track[k], dtype=type(track[k][0]))[rng]
                    for k in track['dynamic']
                },
                static=track['static'],
                dynamic=track['dynamic'],
            )

            # Handle MMSI indexing after segmentation
            mmsi_value = segmented_track.get("mmsi")
            if mmsi_value:
                if mmsi_value not in mmsi_count:
                    mmsi_count[mmsi_value] = 0
                else:
                    mmsi_count[mmsi_value] += 1

                # Modify the mmsi value to attach an index
                segmented_track["mmsi"] = f"{mmsi_value}-{mmsi_count[mmsi_value]}"

            # Yield the segmented track with modified mmsi
            yield segmented_track


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

def min_track_length_filter(tracks, min_length=300):
    for track in tracks:
        if len(track['time']) >= min_length:
            yield track
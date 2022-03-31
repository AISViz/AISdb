''' generation, segmentation, and filtering of vessel trajectories

    .. automethod:: _score_fcn
'''

from functools import reduce
from datetime import timedelta
import warnings

import numpy as np

from gis import haversine, delta_knots
from proc_util import _segment_rng


def TrackGen(
    rowgen: iter,
    colnames: list = [
        'mmsi', 'time', 'lon', 'lat', 'imo', 'vessel_name', 'dim_bow',
        'dim_stern', 'dim_port', 'dim_star', 'ship_type', 'ship_type_txt'
    ]
) -> dict:
    ''' generator converting sets of rows sorted by MMSI to a
        dictionary containing track column vectors.
        each row contains columns from database: mmsi time lon lat name ...
        rows must be sorted by first by mmsi, then time

        args:
            rowgen (aisdb.database.dbqry.DBQuery.gen_qry())
                DBQuery rows generator. Yields rows returned
                by a database query
            colnames (list of strings)
                description of each column in rows.
                first two columns must be ['mmsi', 'time']
            deduplicate_timestamps: bool
                deprecated, this feature may be removed in a future version

        yields:
            dictionary containing track column vectors.
            static data (e.g. mmsi, name, geometry) will be stored as
            scalar values

        >>> from datetime import datetime
        >>> from aisdb import dbpath, DBQuery
        >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi

        >>> q = DBQuery(callback=in_timerange_validmmsi,
        ...             start=datetime(2022, 1, 1),
        ...             end=datetime(2022, 1, 7),
        ...             )

        >>> q.check_idx()  # build index if necessary
        >>> print(f'iterating over rows returned from {dbpath}')
        >>> rowgen = q.gen_qry()

        >>> from aisdb import TrackGen
        >>> for track in TrackGen(rowgen):
        ...     print(track['mmsi'])
        ...     print(f'messages in track segment: {track["time"].size}')
        ...     print(f'keys: {track.keys()}')
    '''
    mmsi_col = [
        i for i, c in zip(range(len(colnames)), colnames)
        if c.lower() == 'mmsi'
    ][0]
    time_col = [
        i for i, c in zip(range(len(colnames)), colnames)
        if c.lower() == 'time'
    ][0]

    staticcols = set(colnames) & set([
        'mmsi',
        'vessel_name',
        'ship_type',
        'ship_type_txt',
        'dim_bow',
        'dim_stern',
        'dim_port',
        'dim_star',
        'imo',
    ])

    dynamiccols = set(colnames) - staticcols

    for rows in rowgen:

        if rows is None or (rows.size <= 1):
            raise ValueError(
                'cannot create vector from zero-size track segment')

        tracks_idx = np.append(
            np.append([0],
                      np.nonzero(rows[:, mmsi_col].astype(int)[1:] !=
                                 rows[:, mmsi_col].astype(int)[:-1])[0] + 1),
            rows.size)

        for i in range(len(tracks_idx) - 1):
            yield dict(
                **{
                    n: (rows[tracks_idx[i]][c] or 0)
                    for c, n in zip(range(len(colnames)), colnames)
                    if n in staticcols
                },
                **{
                    n: rows[tracks_idx[i]:tracks_idx[i + 1]].T[c]
                    for c, n in zip(range(len(colnames)), colnames)
                    if n in dynamiccols
                },
                static=staticcols,
                dynamic=dynamiccols,
            )


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
    dm = max(haversine(*xy1, *xy2), 1)

    # elapsed time between coordinate pair timestamps (seconds)
    dt = max(abs(t2 - t1), 1)

    # computed speed between coordinate pairs (knots)
    ds = (dm / dt) * 1.9438444924406

    if ds < speed_threshold and dm < distance_threshold:
        score = (distance_threshold / dm) / dt
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
    return np.where(scores == np.max(scores))[0][-1]


def encode_greatcircledistance(
    tracks,
    *,
    maxdistance,
    cuttime,
    speed_threshold=50,
    minscore=1e-6,
):
    ''' partitions tracks where delta speeds exceed speed_threshold.
        concatenates track segments with the highest likelihood of being
        sequential, as encoded by a distance/time score function

        args:
            tracks (aisdb.track_gen.TrackGen)
                track vectors generator
            maxdistance (int)
                distance in meters that will be used as a
                speed score numerator
            cuttime (datetime)
                will be converted to epoch and used as a speed
                score denominator
            minscore (float)
                minimum score threshold at which to allow track
                segments to be linked
    '''
    n = 0
    for track in tracks:

        if len(track['time']) <= 1:
            continue

        segments_idx = reduce(
            np.append, ([0], np.where(delta_knots(track) > speed_threshold)[0] + 1,
                        [track['time'].size]))

        pathways = []
        for i in range(segments_idx.size - 1):
            if len(pathways) > 100:
                print(f'excessive number of pathways! mmsi={track["mmsi"]}')
                yield pathways.pop(0)
            scores = np.array([
                _score_fcn(
                    xy1=(track['lon'][segments_idx[i]],
                         track['lat'][segments_idx[i]]),
                    xy2=(pathway['lon'][-1], pathway['lat'][-1]),
                    t1=track['time'][segments_idx[i]],
                    t2=pathway['time'][-1],
                    distance_threshold=maxdistance,
                    speed_threshold=speed_threshold,
                ) for pathway in pathways
            ],
                              dtype=np.float16)

            highscore = scores[np.where(scores == np.max(scores))[0]
                               [0]] if scores.size > 0 else minscore

            if (highscore > minscore):
                pathways[_score_idx(scores)] = dict(
                    **{k: track[k]
                       for k in track['static']},
                    **{
                        k: np.append(
                            pathways[_score_idx(scores)][k],
                            track[k][segments_idx[i]:segments_idx[i + 1]])
                        for k in track['dynamic']
                    },
                    static=track['static'],
                    dynamic=track['dynamic'],
                )

                if pathways[_score_idx(scores)]['time'].size > 10000:
                    pathways[_score_idx(scores)]['label'] = n
                    pathways[_score_idx(scores)]['static'] = set(
                        pathways[_score_idx(scores)]['static']).union(
                            {'label'})
                    yield pathways.pop(_score_idx(scores))
                    n += 1

            else:
                pathways.append(
                    dict(
                        **{k: track[k]
                           for k in track['static']},
                        **{
                            k: track[k][segments_idx[i]:segments_idx[i + 1]]
                            for k in track['dynamic']
                        },
                        static=track['static'],
                        dynamic=track['dynamic'],
                    ))

        for pathway, label in zip(pathways, range(n, len(pathways) + n)):
            pathway['label'] = label
            pathway['static'] = set(pathway['static']).union({'label'})
            assert 'label' in pathway.keys()
            yield pathway


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
            #track['dynamic'].update(['in_zone'])
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

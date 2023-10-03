import warnings
from functools import reduce

import numpy as np

from aisdb.aisdb import encoder_score_fcn
from aisdb.gis import delta_knots, delta_meters


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
    scores = np.array(
        [
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
        dtype=np.float32,
    )
    highscore = (scores[np.where(
        scores == np.max(scores))[0][0]] if scores.size > 0 else minscore)
    return scores, highscore


def _append_highscore(track, *, highscoreidx, pathways, i, segments_idx):
    return dict(
        **{k: track[k]
           for k in track['static']},
        **{
            k:
            np.append(pathways[highscoreidx][k],
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


def encode_score(track, distance_threshold, speed_threshold, minscore):
    ''' Encodes likelihood of persistent track membership when given distance,
        speed, and score thresholds, using track speed deltas computed using
        distance computed by haversine function divided by elapsed time

        A higher distance threshold will increase the maximum distance in
        meters allowed between pings for same trajectory membership. A higher
        speed threshold will allow vessels travelling up to this value in knots
        to be kconsidered for persistent track membership.
        The minscore assigns a minimum score needed to be considered for
        membership, typically 0 or very close to 0 such as 1e-5.

        For example: a vessel travelling at a lower speed with short intervals
        between pings will have a higher likelihood of persistence.
        A trajectory with higher average speed or long intervals between
        pings may indicate two separate trajectories and will be segmented
        forming alternate trajectories according to highest likelihood of
        membership.
    '''
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
    ''' Partitions tracks where delta speeds exceed speed_threshold or
        delta_meters exceeds distance_threshold.
        concatenates track segments with the highest likelihood of being
        sequential, as encoded by the encode_score function

        args:
            tracks (aisdb.track_gen.TrackGen)
                track vectors generator
            distance_threshold (int)
                distance in meters that will be used as a
                speed score numerator
            speed_threshold (float)
                maximum speed in knots that should be considered a continuous
                trajectory
            minscore (float)
                minimum score threshold at which to allow track
                segments to be linked. Value range: (0, 1).
                A minscore closer to 0 will be less restrictive towards
                trajectory grouping. A reasonable value for this is 1e-6.
                This score is computed by the function
                :func:`aisdb.denoising_encoder.encode_score`

        >>> import os
        >>> from datetime import datetime, timedelta
        >>> from aisdb import SQLiteDBConn, DBQuery, TrackGen
        >>> from aisdb import decode_msgs, encode_greatcircledistance, sqlfcn_callbacks

        >>> # create example database file
        >>> dbpath = 'encoder_test.db'
        >>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv',
        ...              'aisdb/tests/testdata/test_data_20211101.nm4']

        >>> with SQLiteDBConn(dbpath) as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn,
        ...                 source='TESTING', verbose=False)

        >>> with SQLiteDBConn(dbpath) as dbconn:
        ...     q = DBQuery(callback=sqlfcn_callbacks.in_timerange_validmmsi,
        ...             dbconn=dbconn,
        ...             start=datetime(2021, 7, 1),
        ...             end=datetime(2021, 7, 7))
        ...     tracks = TrackGen(q.gen_qry(), decimate=True)
        ...     for track in encode_greatcircledistance(
        ...             tracks,
        ...             distance_threshold=250000,  # metres
        ...             speed_threshold=50,         # knots
        ...             minscore=0,
        ...         ):
        ...         print(track['mmsi'])
        ...         print(track['lon'], track['lat'])
        ...         break
        204242000
        [-8.931666] [41.45]

    '''
    '''
        >>> os.remove(dbpath)
    '''
    for track in tracks:
        assert isinstance(track, dict), f'got {type(track)} {track}'
        for path in encode_score(track, distance_threshold, speed_threshold,
                                 minscore):
            yield path

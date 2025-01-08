import os
import warnings
from functools import reduce
import numpy as np
from aisdb.aisdb import encoder_score_fcn
from aisdb.gis import delta_knots, delta_meters
from aisdb.webdata.shore_dist import download_unzip

import geopandas as gpd
from shapely.geometry import Point, MultiPoint
from shapely import prepare
import pickle

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

    '''
    '''
        >>> os.remove(dbpath)
    '''
    for track in tracks:
        assert isinstance(track, dict), f'got {type(track)} {track}'
        for path in encode_score(track, distance_threshold, speed_threshold,
                                 minscore):
            yield path


def remove_pings_wrt_speed(tracks, speed_threshold):
    '''
        Remove pings from tracks where the speed of a vessel
        is lesser than equal to speed_threshold.
        In most cases, the archored vessel tracks are removed through this technique

        args:
            tracks (aisdb.track_gen.TrackGen)
                track vectors generator

        return generator
    '''

    tr1_ = next(tracks)
    columns_ = [ky for ky, vall in tr1_.items() if isinstance(vall, np.ndarray)]

    def update_dict_(tr_):
        indexes_ = np.where(tr_['sog'] <= speed_threshold)
        if len(tr_['sog']) != len(indexes_[0]):
            if len(indexes_[0]) > 0:
                for col in columns_:
                    tr_[col] = np.delete(tr_[col], indexes_)

            yield tr_

    yield from update_dict_(tr1_)
    for tr__ in tracks:
        assert isinstance(tr__, dict), f'got {type(tr__)} {tr__}'
        yield from update_dict_(tr__)


class InlandDenoising:
    data_url = "http://bigdata5.research.cs.dal.ca/geo_land_water_NorthAmerica.7z"

    def __init__(self, data_dir, land_cache='land.pkl', water_cache='water.pkl'):
        download_unzip(self.data_url, data_dir, bytesize=337401807)
        self.land_path = os.path.join(data_dir, land_cache)
        self.water_path = os.path.join(data_dir, water_cache)
        assert os.path.isfile(self.land_path), f"Land file not found at {self.land_path}"
        assert os.path.isfile(self.water_path), f"Water file not found at {self.water_path}"

        # Load geometries during initialization
        with open(self.land_path, 'rb') as f1, open(self.water_path, 'rb') as f2:
            self.land_geom = pickle.load(f1)
            self.water_geom = pickle.load(f2)
            prepare(self.land_geom)
            prepare(self.water_geom)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Cleanup if needed
        pass

    def filter_noisy_points(self, tracks: iter) -> dict:
        """
        Filter points that fall in land but not in water features.
        """
        print("Processing trajectories...")
        for i, traj in enumerate(tracks):
            # Create points for batch processing
            try:
                points = MultiPoint([(lon, lat) for lon, lat in zip(traj['lon'], traj['lat'])])
            except Exception as e:
                print(f"Error creating MultiPoint at trajectory {i}: {e}")
                continue

            # Find points in land but not in water
            try:
                if hasattr(points, 'geoms'):
                    noisy_mask = [
                        self.land_geom.contains(point) and not self.water_geom.contains(point)
                        for point in points.geoms
                    ]
                else:
                    noisy_mask = [self.land_geom.contains(points) and not self.water_geom.contains(points)]
            except Exception as e:
                print(f"Error checking points at trajectory {i}: {e}")
                continue

            noisy_indices = np.where(noisy_mask)[0]

            # Create boolean mask for clean points
            clean_mask = np.ones(len(traj['time']), dtype=bool)
            clean_mask[noisy_indices] = False

            # Create cleaned trajectory
            cleaned_traj = dict(
                **{k: traj[k] for k in traj['static']},
                **{k: traj[k][clean_mask] for k in traj['dynamic']},
                static=traj['static'],
                dynamic=traj['dynamic']
            )

            if len(cleaned_traj['time']) > 0:
                yield cleaned_traj

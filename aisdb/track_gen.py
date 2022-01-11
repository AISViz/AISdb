''' generation, segmentation, and filtering of vessel trajectories '''

from functools import reduce
from datetime import timedelta
import json

from gis import delta_knots

import numpy as np
from gis import haversine, delta_knots

#
# utility functions


def filtermask(track, rng, filters, first_val=False):
    '''
    >>>
        from .gis import compute_knots
        filters=[
                lambda track, rng: track['time'][rng][:-1] != track['time'][rng][1:],
                #lambda track, rng: compute_knots(track, rng) < 40,
                lambda track, rng: (compute_knots(track, rng[:-1]) < 40) & (compute_knots(track, rng[1:]),
                lambda track, rng: np.full(len(rng)-1, 201000000 <= track['mmsi'] < 776000000, dtype=np.bool),
            ]

    '''
    mask = reduce(np.logical_and, map(lambda f: f(track, rng), filters))
    #return np.logical_and(np.append([True], mask), np.append(mask, [True]))
    return np.append([first_val], mask).astype(bool)


def segment_rng(track: dict, maxdelta: timedelta, minsize: int) -> filter:
    assert isinstance(track, dict), f'wrong track type {type(track)}:\n{track}'
    splits_idx = lambda track: np.append(
        np.append([0],
                  np.nonzero(track['time'][1:] - track['time'][:-1] >= maxdelta
                             .total_seconds() / 60)[0] + 1),
        [track['time'].size])
    return filter(
        lambda seg: len(seg) >= minsize,
        list(map(range,
                 splits_idx(track)[:-1],
                 splits_idx(track)[1:])))


''' chainable track generators '''


def trackgen(
    rowgen: iter,
    colnames: list = [
        'mmsi',
        'time',
        'lon',
        'lat',
        # 'cog',
        # 'sog',
        # 'msgtype',
        'imo',
        'vessel_name',
        'dim_bow',
        'dim_stern',
        'dim_port',
        'dim_star',
        # 'ship_type',
        # 'ship_type_txt',
    ],
    deduplicate_timestamps: bool = True,
) -> dict:
    '''
        each row contains columns from database: mmsi time lon lat name ...
        rows must be sorted by first by mmsi, then time

        colnames is a description of each column in rows.
        first two columns must be ['mmsi', 'time']

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
        # 'ship_type',
        # 'ship_type_txt',
        'dim_bow',
        'dim_stern',
        'dim_port',
        'dim_star',
        # 'mother_ship_mmsi',
        # 'part_number',
        # 'vendor_id',
        # 'model',
        # 'serial',
        'imo',
        # 'deadweight_tonnage', 'submerged_hull_m^2',
    ])

    dynamiccols = set(colnames) - staticcols  # - set(['mmsi', 'time'])

    for rows in rowgen:

        if deduplicate_timestamps:
            dupe_idx = np.append(
                [False],
                np.logical_and(
                    rows[:, time_col].astype(float).astype(int)[:-1] ==
                    rows[:, time_col].astype(float).astype(int)[1:],
                    rows[:, mmsi_col].astype(float).astype(int)[:-1] ==
                    rows[:, mmsi_col].astype(float).astype(int)[1:]))
            rows = np.delete(rows, dupe_idx, axis=0)

        tracks_idx = np.append(
            np.append([0],
                      np.nonzero(rows[:, mmsi_col].astype(int)[1:] !=
                                 rows[:, mmsi_col].astype(int)[:-1])[0] + 1),
            rows.size)

        for i in range(len(tracks_idx) - 1):
            # assert len(rows[tracks_idx[i]:tracks_idx[i+1]].T[1]) \
            # == len(np.unique(rows[tracks_idx[i]:tracks_idx[i+1]].T[1]))
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


def segment_tracks_timesplits(tracks, maxdelta=timedelta(hours=2), minsize=1):
    for track in tracks:
        for rng in segment_rng(track, maxdelta, minsize):
            yield dict(
                **{k: track[k]
                   for k in track['static']},
                **{k: track[k][rng]
                   for k in track['dynamic']},
                static=track['static'],
                dynamic=track['dynamic'],
            )


def segment_tracks_encode_greatcircledistance(tracks,
                                              maxdistance,
                                              cuttime,
                                              cutknots=50,
                                              minscore=1e-6):
    ''' if the distance between two consecutive points in the track exceeds
        given threshold, the track will be segmented '''
    '''
    score_fcn = lambda xy1,xy2,t1,t2,distance_meters=maxdistance: (
                            #((distance_meters) / max(5, haversine(*xy1, *xy2))) / max(2, np.abs(t2-t1))
                            ((distance_meters) / max(5, dm)) / max(2, dt)
                            if
                            (dm := haversine(*xy1, *xy2)) < maxdistance
                            and (dt := abs(t2-t1)) < cuttime.total_seconds() / 60
                            else -1
                        )
    '''
    score_fcn = lambda xy1, xy2, t1, t2, distance_meters=maxdistance: (
        (distance_meters / dm) / dt
        if (dm := max(haversine(*xy1, *xy2), 2)) / (
            (dt := max(abs(t2 - t1), 2)
             )) * 1.9438444924406 < cutknots and dm < distance_meters
        #and dt < cuttime.total_seconds() / 60
        else -1)

    score_idx = lambda scores: np.where(scores == np.max(scores))[0][-1]
    n = 0
    for track in tracks:
        # segments_idx = np.nonzero(np.array(list(map(haversine,
        # track['lon'][:-1], track['lat'][:-1],
        # track['lon'][1:], track['lat'][1:]))) > 5000)[0]+1

        segments_idx = reduce(
            np.append, ([0], np.where(delta_knots(track) > cutknots)[0] + 1,
                        [track['time'].size]))

        pathways = []
        for i in range(segments_idx.size - 1):
            if len(pathways) > 100:
                print(f'excessive number of pathways! mmsi={track["mmsi"]}')
                yield pathways.pop(0)
            scores = np.array([
                score_fcn(xy1=(track['lon'][segments_idx[i]],
                               track['lat'][segments_idx[i]]),
                          xy2=(pathway['lon'][-1], pathway['lat'][-1]),
                          t1=track['time'][segments_idx[i]],
                          t2=pathway['time'][-1]) for pathway in pathways
            ],
                              dtype=np.float16)
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

            highscore = scores[np.where(scores == np.max(scores))[0]
                               [0]] if scores.size > 0 else minscore

            if (highscore > minscore):
                pathways[score_idx(scores)] = dict(
                    **{k: track[k]
                       for k in track['static']},
                    **{
                        k: np.append(
                            pathways[score_idx(scores)][k],
                            track[k][segments_idx[i]:segments_idx[i + 1]])
                        for k in track['dynamic']
                    },
                    static=track['static'],
                    dynamic=track['dynamic'],
                )

                if pathways[score_idx(scores)]['time'].size > 10000:
                    pathways[score_idx(scores)]['label'] = n
                    pathways[score_idx(scores)]['static'] = set(
                        pathways[score_idx(scores)]['static']).union({'label'})
                    yield pathways.pop(score_idx(scores))
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
            #for i in range(pathway['time'].size-1):
            #    assert pathway['time'][i] < pathway['time'][i+1]
            yield pathway


def mmsirange(tracks, low=200000000, high=780000000):
    for track in tracks:
        if low <= track['mmsi'] <= high:
            yield track
        elif track['mmsi'] > high:
            return


def mmsifilter(rowgen, mmsis=[]):
    for row in rowgen:
        print(row[0][0], end='\r')
        if row[0][0] in mmsis:
            yield row
        if sum([row[0][0] < mmsi for mmsi in mmsis]) == len(mmsis):
            continue
        elif sum([row[0][0] > mmsi for mmsi in mmsis]) == len(mmsis):
            print()
            return


def max_tracklength(tracks, max_track_length=100000):
    ''' applies a maximum track length to avoid excess memory consumption

        args:
            tracks: generator
                yields track dictionaries
            max_track_length: int
                tracks exceeding this number of datapoints will be segmented

        yields track dictionaries
    '''

    for track in tracks:
        # apply upper limit to track size to improve memory performance in worst-case scenario
        while (track['time'].size > max_track_length):
            yield dict(
                **{k: track[k]
                   for k in track['static']},
                **{k: track[k][:max_track_length]
                   for k in track['dynamic']},
                static=track['static'],
                dynamic=set(track['dynamic']),
            )
            track = dict(
                **{k: track[k]
                   for k in track['static']},
                **{k: track[k][max_track_length:]
                   for k in track['dynamic']},
                static=track['static'],
                dynamic=set(track['dynamic']),
            )
        yield track


def concat_realisticspeed(tracks, knots_threshold=50):
    ''' if two consecutive tracks are within a realistic speed threshold, they will be concatenated '''
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


def concat_occurs_after(tracks, grace_period=300):
    ''' concatenate sequential tracks

        if two consecutive tracks have the same mmsi and are temporally
        sequential, concatenate them

        by default allow a grace period to account for receiver delay
    '''
    segment = next(tracks)
    for track in tracks:
        if (segment['mmsi'] == track['mmsi']
                and segment['time'][-1] - grace_period < track['time'][0]):
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
    ''' compute points-in-polygons for track positional reports in domain polygons

        yields track dictionaries
    '''
    for track in tracks:
        if not 'in_zone' in track.keys():
            track['in_zone'] = np.array([
                domain.point_in_polygon(x, y)
                for x, y in zip(track['lon'], track['lat'])
            ],
                                        dtype=object)
            track['dynamic'] = set(track['dynamic']).union(set(['in_zone']))
        yield track


def tracks_transit_frequency(tracks, domain):
    for track in tracks:
        if 'in_zone' not in track.keys():
            track['in_zone'] = np.array([
                domain.point_in_polygon(x, y)
                for x, y in zip(track['lon'], track['lat'])
            ],
                                        dtype=object)
            track['dynamic'] = set(track['dynamic']).union(set(['in_zone']))

        count_transit_nodes = sum(
            np.nonzero(track['in_zone'][1:] != track['in_zone'][:-1])[0])
        delta_minutes = (track['time'][-1] - track['time'][0]) or 1
        track['hourly_transits_avg'] = count_transit_nodes / (delta_minutes /
                                                              60)
        track['static'] = set(track['static']).union(
            set(['hourly_transits_avg']))
        yield track.copy()


def log_track(track):

    default = lambda obj: str(obj) if not isinstance(obj, (list, np.ndarray)
                                                     ) else list(obj)
    jsonify = lambda obj: json.dumps(obj, default=default, sort_keys=True)

    pass


def filter_tracks(
        tracks,
        filter_callback=lambda track: track['hourly_transits_avg'] > 6,
        logging_callback=lambda track: False):
    for track in tracks:
        if logging_callback(track):
            log_track(track)
        if filter_callback(track):
            continue
        else:
            yield track

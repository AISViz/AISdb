from gis import delta_knots#, delta_meters

import numpy as np
from sklearn.cluster import DBSCAN


def flag(track):
    ''' returns True if any computed speed deltas exceed 50 knots ''' 

    if len(track['time']) == 1: 
        return False

    if np.max(delta_knots(track, range(len(track['time'])))) > 50:
        return True

    return False


def segment_tracks_dbscan(track_dicts, max_cluster_dist_km=50, flagfcn=flag):
    '''
        args:
            track_dicts: iterable
                iterable containing vessel trajectory dictionaries
            max_cluster_dist_km:
                approximate distance of how far points should be spread apart 
                while still considered to be part of the same cluster.
                should be chosen relative to the smallest radius of network 
                graph node polygons
            flagfcn:
                callback function that accepts a track dictionary and returns
                True or False. if True, the track will be clustered.
                if False, the track will yield unchanged

        yields: 
            clustered subset trajectories where computed speed deltas exceed 50 knots
    '''
        
    for track in track_dicts:

        if len(track['time']) == 1: continue

        if not flagfcn(track):
            yield track

        else:
            # set epsilon to clustering distance, convert coords to radian, cluster with haversine metric
            epsilon = max_cluster_dist_km / 6367  # 6367km == earth circumference 
            yx = np.vstack((list(map(np.deg2rad, track['lat'])), list(map(np.deg2rad, track['lon'])))).T
            clusters = DBSCAN(eps=epsilon, min_samples=1, algorithm='brute', metric='haversine').fit(yx)

            # yield track subsets assigned to cluster labels
            for l in set(clusters.labels_):

                mask = clusters.labels_ == l

                clustered = dict(
                        mmsi            = track['mmsi'],
                        time            = track['time'][mask],
                        cluster_label   = l,
                        static          = track['static'].union(set(['cluster_label'])),
                        dynamic         = track['dynamic'],
                        **{k:track[k] for k in track['static'] - set(['cluster_label'])},
                        **{k:track[k][mask] for k in track['dynamic']},
                    )

                yield clustered



'''
                if not flagfcn(clustered):
                    yield clustered
                else:
                    for recursive_clustered in segment_tracks_dbscan([clustered], max_cluster_dist_km / 3, flagfcn,_):
                        print(f'warning: recursively clustering {recursive_clustered["mmsi"]}\t'
                              f'cluster distance: {max_cluster_dist_km / 2}km')
                        yield recursive_clustered
'''

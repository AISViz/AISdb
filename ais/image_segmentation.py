from gis import delta_knots 

import numpy as np
from sklearn.cluster import DBSCAN


def flag(track):

    if len(track['time']) == 1: 
        return False

    if not 'sog_computed' in track.keys():
        track['sog_computed'] = delta_knots(track, range(len(track['time'])))

    if np.max(track['sog_computed']) > 50:
        return True

    return False


def cluster_duplicate_mmsis(track_dicts, max_cluster_dist_km=75, flagfcn=flag):

    #tracks = iter(track_dicts)

    for track in track_dicts:
        if not flagfcn(track):
            yield track
        else:
            # set epsilon to 50km clustering distance and convert coords to radian
            # 50km chosen relative to the smallest radius of network graph node polygons
            # 6367km == earth circumference
            epsilon = max_cluster_dist_km / 6367
            yx = np.vstack((list(map(np.deg2rad, track['lat'])), list(map(np.deg2rad, track['lon'])))).T

            # cluster using great circle distance as metric
            clusters = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(yx)

            # yield track subsets assigned to cluster labels
            for l in set(clusters.labels_):
                mask = clusters.labels_ == l
                yield dict(
                        mmsi=track['mmsi'],
                        time=track['time'][mask],
                        cluster_label=l,
                        static=track['static'].union(set(['cluster_label'])),
                        dynamic=track['dynamic'].union(set(['sog_computed'])),
                        **{k:track[k] for k in track['static']},
                        **{k:track[k][mask] for k in track['dynamic']},
                    )


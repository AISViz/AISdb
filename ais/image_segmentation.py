''' https://en.wikipedia.org/wiki/Image_segmentation#Clustering_methods 
    https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/

'''


import numpy as np
from sklearn.cluster import DBSCAN
from shapely.geometry

great_circle = lambda xx, yy : haversine(x[0], y[0], x[1], y[1])  # metres


assert track_dicts

def flag(track):

    if len(track['time']) == 1: 
        return False

    if not 'sog_computed' in track.keys():
        track['sog_computed'] = delta_knots(track, range(len(track['time'])))

    if np.max(track['sog_computed']) > 50:
        return True

    return False


def cluster_duplicate_mmsis(track_dicts):

    tracks = iter(track_dicts)

    for track in tracks:
        if not flag(track):
            yield track

        else:
            
            elapsed = (track['time'][-1] - track['time'][0]) * 60
            max_km = (50 * (elapsed * 1.943844 )) / 1000
            epsilon = (max_km / 6367) # km per radian

            yx = np.vstack((list(map(np.deg2rad, track['lat'])), list(map(np.deg2rad, track['lon'])))).T
            db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(yx)
            labels = db.labels_
            len(set(labels))


    pass





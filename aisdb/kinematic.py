import numpy as np
import scipy
import math
import warnings
import aisdb
from webdata import load_raster

def feature(tracks, type='all'):
    """
    Calculate the kinematic feature of a track.

    Args:
        track (generator): The track to calculate the feature.
        type (str or list): The type of the feature to calculate.
    """
    if type == 'all':
        type = ['ccc', 'ccs', 'distance', 'cumulative_distance', 'speed_diff', 'acceleration', 'jerk']
    type_mapping = {
        'ccc': _calculate_course,
        'ccs': _calculate_speed,
        'distance': _distance,
        'cumulative_distance': _cumulative_distance,
        'course_diff': _course_diff,
        'speed_diff': _speed_diff,
        'acceleration': _acceleration,
        'jerk': _jerk,
    }

    for track in tracks:
        # skipping invalid or single-point track records
        if 'time' not in track or 'dynamic' not in track:
            warnings.warn('Track is missing required keys (time or dynamic), skipping...')
            continue

        if track['time'].size <= 1:
            # yield track
            warnings.warn('cannot calculate kinematic features for track of length 1, skipping...')
            continue

        # sort time and dynamic data by time
        sorted_indices = np.argsort(track['time'])

        # incorporate kinematic features from calculations
        valid_features = []
        for key in track['dynamic']:   # attach original dynamic keys
            track[key] = track[key][sorted_indices]
        for new_feature in type:
            if new_feature in type_mapping:
                track[new_feature] = type_mapping[new_feature](track)
                valid_features.append(new_feature)
            else:
                warnings.warn(f'unknown feature type: {new_feature}, skipping...')

        track['dynamic'] = set(track['dynamic']).union(set(valid_features))

        yield track


def _calculate_course(track):
    """
    Calculate the course of the track.
    """


    pass

def _course_diff(track):
    """
    Calculate the course difference of the track.
    """
    pass

def _calculate_speed(track):
    """
    Calculate the speed of the track.
    :return NumPy array: The speed of each track point.
    """
    time_diff = np.diff(track['time'].astype('datetime64[s]')) / np.timedelta64(1, 'h')  # Convert to hours
    dist = _distance(track)   # this should in nautic miles or in meters? (Assume it's in meters)
    dist_nm = 0.000539957 * dist
    # Calculate speed in knots (nautical miles per hour)
    speed = dist_nm / time_diff   # speed is in knots

    # Pad with last value to match original array length
    return np.pad(speed, (0, 1), 'edge')


def _speed_diff(track):
    """
    Calculate the speed difference of the track.
    """
    pass

def _acceleration(track):
    """
    Calculate the acceleration (in knots) at each point of the track.
    """
    pass

def _jerk(track):
    """
    Calculate the jerk (in knots/hour^2) at each point of the track.
    Jerk measures the change of an object's acceleration over time.
    :return:
    """
    pass

def _distance(track):
    """
    Calculate the distance of the track.
    """
    R = 3440.065  # Earth's radius in nautical miles

    lat1 = np.radians(track['lat'][:-1])
    lat2 = np.radians(track['lat'][1:])
    lon1 = np.radians(track['lon'][:-1])
    lon2 = np.radians(track['lon'][1:])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distances = R * c

    # Pad with zero at the start to match original array length
    return np.pad(distances, (0, 1), 'constant')

def _cumulative_distance(track):
    """
    Calculate the cumulative distance of the track.
    """
    distances = _distance(track)
    return np.cumsum(distances)

# def _bathymetry(track):
#     """
#     Calculate the bathymetry of the track.
#     """
#     pass

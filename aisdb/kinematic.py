import numpy as np
import scipy
import math
import warnings
import aisdb
from webdata import load_raster

def feature(tracks, type='all', raster=False):
    """
    Calculate the kinematic feature of a track.

    Args:
        track (generator): The track to calculate the feature.
        type (str or list): The type of the feature to calculate.
        raster (bool): Whether to attach the raster features, including bathymetry, distance to shore and to the nearest port.
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
        'bathymetry': _bathymetry,
        'distance_to_shore': _distance_to_shore,
        'distance_to_port': _distance_to_port
    }
    if raster:
        type.append(['bathymetry', 'distance_to_shore', 'distance_to_port'])

    for track in tracks:
        if track['time'].size <= 1:
            # yield track
            warnings.warn('cannot calculate kinematic features for track of length 1, skipping...')
            continue

        # Sort time and dynamic data by time
        sorted_indices = np.argsort(track['time'])
        
        for key in track['dynamic']:
            track[key] = track[key][sorted_indices]
        for feature in type:
            if feature in type_mapping:
                track[feature] = type_mapping[feature](track)
            else:
                warnings.warn(f'unknown feature type: {feature}, skipping...')
        
        yield track
    
    return


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
    """
    pass

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
    Calculate the jerk (in knots/hour) at each point of the track.
    """
    pass

def _distance(track):
    """
    Calculate the distance of the track.
    """
    pass

def _cumulative_distance(track):
    """
    Calculate the cumulative distance of the track.
    """
    pass

def _bathymetry(track):
    """
    Calculate the bathymetry of the track.
    """
    pass

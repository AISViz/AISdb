import os
import sys
import pickle
from datetime import datetime

import numpy as np

from ..track_gen import *


assert sys.version_info.major >= 3, 'python >= 3.8 required'
assert sys.version_info.major > 3 or sys.version_info.minor >= 8, 'python >= 3.8 required'


def test_track_segments():
    # load test dataset
    # note that rows are sorted by mmsi and time, as output by the database
    with open('test/testdata/longtrack.pickle', 'rb') as f: testrows = pickle.load(f)

    # TODO: update track segment function to allow a variable number of columns
    # currently requires rows to have columns: mmsi time lon lat cog sog vessel_name vessel_type

    '''
    >>> testrows 
    array([[331184000, datetime.datetime(2020, 10, 1, 0, 0, 36), -54.1210716667, 68.73799, 27.1, 2.7, 'POLAR QAASIUT', 'Fishing'],
           [331184000, datetime.datetime(2020, 10, 1, 0, 0, 36), -54.1210716667, 68.73799, 27.1, 2.7, 'POLAR QAASIUT', 'Fishing'],
           [331184000, datetime.datetime(2020, 10, 1, 0, 1, 32), -54.1203316667, 68.7385533333, 25.1, 2.5, 'POLAR QAASIUT', 'Fishing'],
           ...,
           [331184000, datetime.datetime(2020, 10, 31, 23, 51, 18), -54.656045, 68.5003616667, 267.3, 7.8, 'POLAR QAASIUT', 'Fishing'],
           [331184000, datetime.datetime(2020, 10, 31, 23, 59, 36), -54.7137016667, 68.5014216667, 273.6, 9.4, 'POLAR QAASIUT', 'Fishing'],
           [331184000, datetime.datetime(2020, 10, 31, 23, 59, 36), -54.7137016667, 68.5014216667, 273.6, 9.4, 'POLAR QAASIUT', 'Fishing']],
          dtype=object)
    '''

    # segment function parameters
    maxdelta    = timedelta(minutes=30)     # time threshold to allow before segmenting tracks
    minsize     = 1                         # minimum segment length

    # each segment can then be filtered for e.g. duplicate timestamps, max speed, etc..
    filters = [
        lambda track, rng: track['time'][rng][:-1] != track['time'][rng][1:],                           # filter duplicate timestamps
        lambda track, rng: np.append(compute_knots(track, rng[:-1]) < 50, [True]),                      # haversine speed threshold
        lambda track, rng: np.append([True], compute_knots(track, rng[ 1:]) < 50),                      # haversine speed threshold
        lambda track, rng: np.full(len(rng)-1, 201000000 <= track['mmsi'] < 776000000, dtype=np.bool),  # valid mmsi range
    ]

    # for large datasets, iterate over generator function
    for track in trackgen(testrows):  # only one track in test set
        segments = segment(track, maxdelta, minsize)
        for rng in segments:
            mask = filtermask(track, rng, filters)  # filters are applied by passing filters to filtermask function
            print(f"segment: {rng}\tsegment length: {len(rng)}\tfiltered: {len(rng)-sum(mask)}\tstart time: {track['time'][rng][0]}\t\tend time: {track['time'][rng][-1]}")

    # or, more concisely without filtering (for datasets that fit in memory)
    tracks_segmented = { track['mmsi'] : dict(**track, seg=list(segment(track, maxdelta, minsize))) for track in trackgen(testrows) }
    
    breakpoint()



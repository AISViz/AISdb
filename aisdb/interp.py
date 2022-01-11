''' linear interpolation of track segments on temporal axis '''

from datetime import timedelta

import numpy as np

from aisdb.gis import dt_2_epoch
from track_gen import segment_rng


def interp_time(tracks,
                start,
                stop,
                step=timedelta(minutes=10),
                maxdelta=timedelta(hours=3),
                minsize=3):
    ''' segment and interpolate tracks to 10 minute intervals

        args:
            tracks:
                dict of messages sorted by mmsi then time.
                uses mmsi as key with columns: time lon lat cog sog name .. etc
            start:
                beginning of temporal range as datetime
            stop:
                end of temporal range as datetime
            step:
                temporal interpolation interval as timedelta
            maxdelta:
                timedelta threshold at which to partition track segments

        returns:
            dictionary of interpolated tracks

        debug:
            cur.scroll(0, mode='absolute')
            rows = cur.fetchall()
    '''
    # timestamp  = np.arange(start, stop+step, step).astype(datetime)
    timestamp = np.arange(dt_2_epoch(start), dt_2_epoch(stop + step),
                          step.total_seconds())
    # intervals  = np.array(list(map(int, map(datetime.timestamp, timestamp))))
    intervals = np.array(list(map(int, timestamp)))
    interpfcn = lambda track, segments, intervals=intervals: {
        **{k: track[k]
           for k in ('mmsi', 'name', 'type')},
        'time':
        timestamp,
        'seg': [
            {
                n: np.interp(
                    x=intervals,
                    # xp=list(map(datetime.timestamp, track['time'][rng])),
                    xp=track['time'][rng],
                    fp=track[n][rng],
                    left=np.nan,
                    right=np.nan,
                    period=None,
                )
                for n in ['lon', 'lat', 'cog', 'sog']
            } for rng in segments
        ],
        'rng': [
            range(
                np.nonzero(timestamp >= track['time'][rng][0])[0][0],
                np.nonzero(timestamp <= track['time'][rng][-1])[0][-1])
            for rng in segments
        ],
    }
    for track in tracks:
        yield interpfcn(track, list(segment_rng(track, maxdelta, minsize)))

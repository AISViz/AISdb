from datetime import datetime, timedelta

import numpy as np

from track_geom import *


def interp_time(tracks, start, stop, step=timedelta(minutes=10), maxdelta=timedelta(hours=3)):
    ''' segment and interpolate tracks to 10 minute intervals
        
        args:
            tracks: 
                dict of messages sorted by mmsi then time. 
                uses mmsi as key with columns: time lon lat cog sog name type.. etc
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
    timestamp  = np.arange(start, stop, step).astype(datetime)
    intervals  = np.array(list(map(int, map(datetime.timestamp, timestamp))))
    interpfcn  = lambda track, segments, intervals=intervals: {
            **{ track[k] :  v for k in ('mmsi','name','type') },
            'time'  :   timestamp,
            'seg'   :   [ { n :   
                    np.interp(
                            x=intervals,
                            xp=list(map(datetime.timestamp, track['time'][rng])),
                            fp=track[n][rng],
                            left=np.nan,
                            right=np.nan,
                            period=None,
                        ) for n in ['lon','lat','cog','sog']
                    } for rng in segments ],
            'rng'   :   [ range( 
                        np.argmax(timestamp >= track['time'][rng][ 0]),
                        np.argmin(timestamp <= track['time'][rng][-1]),
                    ) for rng in segments ], 
        }
    for track in tracks: yield interpfcn(track, list(segment(track, maxdelta)))


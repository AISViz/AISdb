''' linear interpolation of track segments on temporal axis '''

from datetime import timedelta

import numpy as np


def np_interp_linear(track, key, intervals):
    return np.interp(
        x=intervals.astype(int),
        xp=track['time'].astype(int),
        fp=track[key].astype(float),
        left=np.nan,
        right=np.nan,
        period=None,
    )


def interp_time(tracks, step=timedelta(minutes=10)):
    ''' linear interpolation on vessel trajectory

        args:
            tracks (dict)
                messages sorted by mmsi then time.
                uses mmsi as key with columns: time lon lat cog sog name .. etc
            step (datetime.timedelta)
                interpolation interval

        returns:
            dictionary of interpolated tracks
    '''
    for track in tracks:

        if track['time'].size <= 1:
            yield track.copy()
            continue

        intervals = np.arange(
            start=track['time'][0],
            stop=track['time'][-1],
            step=int(step.total_seconds()),
        ).astype(int)

        itr = dict(
            **{k: track[k]
               for k in track['static']},
            time=intervals,
            static=track['static'],
            dynamic=track['dynamic'],
            **{
                k: np_interp_linear(track, k, intervals)
                for k in track['dynamic'] if k != 'time'
            },
        )
        yield itr.copy()

    return

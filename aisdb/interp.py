''' linear interpolation of track segments on temporal axis '''

from datetime import timedelta

import numpy as np
import warnings


def np_interp_linear(track, key, intervals):
    assert len(track['time']) == len(track[key])
    return np.interp(x=intervals.astype(int),
                     xp=track['time'].astype(int),
                     fp=track[key].astype(float))


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
            # yield track
            warnings.warn('cannot interpolate track of length 1, skipping...')
            continue

        intervals = np.arange(
            start=track['time'][0],
            stop=track['time'][-1] + int(step.total_seconds()),
            step=int(step.total_seconds()),
        ).astype(int)

        assert len(intervals) >= 1

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
        yield itr

    return


async def interp_time_async(tracks, step=timedelta(minutes=10)):
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
    async for track in tracks:

        if track['time'].size <= 1:
            # yield track
            warnings.warn('cannot interpolate track of length 1, skipping...')
            continue

        intervals = np.arange(
            start=track['time'][0],
            stop=track['time'][-1] + int(step.total_seconds()),
            step=int(step.total_seconds()),
        ).astype(int)

        yield dict(
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

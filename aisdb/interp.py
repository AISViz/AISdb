''' linear interpolation of track segments on temporal axis '''

import warnings
from datetime import timedelta

import numpy as np
from pyproj import Transformer, Geod

from scipy.interpolate import CubicSpline


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

        Example:
            >>> import numpy as np
            >>> from datetime import timedelta, datetime
            >>> import aisdb

            >>> y1, x1 = -66.84683, -61.10595523571155
            >>> y2, x2 = -66.83036, -61.11595523571155
            >>> y3, x3 = 48.2815186388735, -61.12595523571155
            >>> t1 = dt_2_epoch( datetime(2021, 1, 1, 1) )
            >>> t2 = dt_2_epoch( datetime(2021, 1, 1, 2) )
            >>> t3 = dt_2_epoch(datetime(2021, 1, 1, 3))

            >>> # creating a sample track
            >>> tracks_short = [
            ...                 dict( lon=np.array([x1, x2, x3]),
            ...                 lat=np.array([y1, y2, y3]),
            ...                 time=np.array([t1, t2, t3]),
            ...                 dynamic=set(['lon', 'lat', 'time']),
            ...                 static = set() ) ]

            >>> tracks__ = aisdb.interp.interp_time(tracks_short, timedelta(minutes=10))
            >>> for tr in tracks__:
            ...     print(tr)

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


def geo_interp_time(tracks, step=timedelta(minutes=10), original_crs=4269):
    ''' Geometric interpolation on vessel trajectory, assumes default EPSG:4269

            args:
                tracks (dict)
                    messages sorted by mmsi then time.
                    uses mmsi as key with columns: time lon lat cog sog name .. etc
                step (datetime.timedelta)
                    interpolation interval

            returns:
                dictionary of interpolated tracks

            Example:
                >>> import numpy as np
                >>> from datetime import timedelta, datetime
                >>> import aisdb

                >>> y1, x1 = -66.84683, -61.10595523571155
                >>> y2, x2 = -66.83036, -61.11595523571155
                >>> y3, x3 = 48.2815186388735, -61.12595523571155
                >>> t1 = dt_2_epoch( datetime(2021, 1, 1, 1) )
                >>> t2 = dt_2_epoch( datetime(2021, 1, 1, 2) )
                >>> t3 = dt_2_epoch(datetime(2021, 1, 1, 3))

                >>> # creating a sample track
                >>> tracks_short = [
                ...                 dict( lon=np.array([x1, x2, x3]),
                ...                 lat=np.array([y1, y2, y3]),
                ...                 time=np.array([t1, t2, t3]),
                ...                 dynamic=set(['lon', 'lat', 'time']),
                ...                 static = set() ) ]

                >>> tracks__ = aisdb.interp.geo_interp_time(tracks_short, timedelta(minutes=10))
                >>> for tr in tracks__:
                ...     print(tr)

        '''
    new_crs = 3857
    fwd_trans = Transformer.from_crs(original_crs, new_crs, always_xy=True)
    back_trans = Transformer.from_crs(new_crs, original_crs, always_xy=True)
    geod = Geod(ellps="WGS84")
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
            dynamic=track['dynamic']
        )
        if 'lat' in track['dynamic']:
            x, y = fwd_trans.transform(track['lon'], track['lat'])
            x = np.interp(x=intervals.astype(int),
                          xp=track['time'].astype(int),
                          fp=x.astype(float))
            y = np.interp(x=intervals.astype(int),
                          xp=track['time'].astype(int),
                          fp=y.astype(float))
            itr['lon'], itr['lat'] = back_trans.transform(x, y)
            if 'cog' in track['dynamic']:
                courses, _, _ = geod.inv(itr['lon'][:-1], itr['lat'][:-1], itr['lon'][1:], itr['lat'][1:])
                itr['cog'] = np.append(courses, track['cog'][-1])
        for key in track['dynamic']:
            if key not in itr:
                itr[key] = np.interp(x=intervals.astype(int),
                                     xp=track['time'].astype(int),
                                     fp=track[key].astype(float))

        yield itr

    return


def interp_spacing(spacing: int, tracks, crs=4269):
    '''linear interpolation on vessel trajectory

        args:
            tracks (dict)
                messages sorted by mmsi then time.
                uses mmsi as key with columns: time lon lat cog sog name .. etc
            spacing (int)
                interpolation interval in meters

        returns:
            dictionary of interpolated tracks

    >>> import numpy as np
    >>> from datetime import timedelta, datetime

    >>> y1, x1 = -66.84683, -61.10595523571155
    >>> y2, x2 = -66.83036, -61.11595523571155
    >>> y3, x3 = -66.82036, -61.12595523571155
    >>> t1 = dt_2_epoch( datetime(2021, 1, 1, 1) )
    >>> t2 = dt_2_epoch( datetime(2021, 1, 1, 2) )
    >>> t3 = dt_2_epoch(datetime(2021, 1, 1, 3))

    >>> # creating a sample track
    >>> tracks_short = [
    ...    dict(
    ...        lon=np.array([x1, x2, x3]),
    ...        lat=np.array([y1, y2, y3]),
    ...        time=np.array([t1, t2, t3]),
    ...        dynamic=set(['lon', 'lat', 'time']),
    ...        static = set()
    ...    )
    ... ]

    >>> tracks__ = aisdb.interp.interp_time(tracks_short, timedelta(minutes=10))
    >>> tracks__ = aisdb.interp.interp_spacing(spacing=1000, tracks=tracks__)
    >>> for tr in tracks__:
    ...    print(tr)
    '''
    crs2 = 3857
    transformer = Transformer.from_crs(crs, crs2, always_xy=True)
    inv_transformer = Transformer.from_crs(crs2, crs, always_xy=True)
    geod = Geod(ellps="WGS84")

    # loop over files if vessel not read into memory, better for large data

    for track in tracks:
        if track['time'].size <= 1:
            # yield track
            warnings.warn('cannot interpolate track of length 1, skipping...')
            continue

        # respace the coordinates

        lon, lat = transformer.transform(track['lon'], track['lat'])
        if len(lon) == 1:
            continue
        xd = np.diff(lon)
        yd = np.diff(lat)
        dist = np.sqrt(xd ** 2 + yd ** 2)
        u = np.cumsum(dist)
        u = np.hstack([[0], u])
        total_dist = u[-1]
        if total_dist <= spacing:
            continue
        t = np.hstack([np.arange(0, total_dist, spacing), [total_dist]])
        # t = np.linspace(0, total_dist, int(number_of_points))
        # interpolate the other attributes back
        track['lon'], track['lat'] = inv_transformer.transform(np.interp(t, u, lon), np.interp(t, u, lat))
        courses, _, _ = geod.inv(track['lon'][:-1], track['lat'][:-1], track['lon'][1:], track['lat'][1:])
        if 'cog' in track:
            track['cog'] = np.append(courses, track['cog'][-1])
        for k in track['dynamic']:
            if k == 'lon' or k == 'lat' or k == 'cog':
                continue
            track[k] = np.interp(t, u, track[k])

        yield track


def cubic_spline(track, key, intervals):
    try:
        # Remove duplicate time values
        unique_times, unique_indices = np.unique(track['time'], return_index=True)
        unique_values = track[key][unique_indices]

        assert len(unique_times) == len(unique_values)

        # Check if time is strictly increasing
        if not np.all(np.diff(unique_times) > 0):
            print("Error: Time values are not in strictly increasing order after removing duplicates.")
            print(f"Current time order: {unique_times}")
            return None

        if len(unique_times) < 2:
            print("Error: Not enough unique time points to perform interpolation.")
            return None

        # Create cubic spline with unique, sorted time and values
        cs = CubicSpline(x=unique_times, y=unique_values)

        return cs(intervals)

    except Exception as e:
        # Print the current time order and the exception message
        print(f"Error occurred in cubic spline interpolation: {e}")
        print(f"Current time order: {track['time']}")
        raise  # Re-raise the exception to ensure the calling function knows an error occurred


def interp_cubic_spline(tracks, step=timedelta(minutes=10)):
    ''' Cubic spline interpolation on vessel trajectory

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

        # Sort time and dynamic data by time
        sorted_indices = np.argsort(track['time'])

        for key in track['dynamic']:
            track[key] = track[key][sorted_indices]

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
                k: cubic_spline(track, k, intervals)
                for k in track['dynamic'] if k != 'time'
            },
        )
        yield itr

    return
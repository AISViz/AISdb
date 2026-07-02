"""interpolation of track segments on temporal or spatial axes.

positional keys (lat/lon) are resampled in projected coordinates
(EPSG:3857) rather than in degrees, then transformed back to the
source CRS. non-positional dynamic keys are resampled linearly on
the temporal axis.
"""

import warnings
from datetime import timedelta

import numpy as np
from pyproj import Transformer, Geod

from scipy.interpolate import CubicSpline

_TO_MERCATOR = Transformer.from_crs(4326, 3857, always_xy=True)
_FROM_MERCATOR = Transformer.from_crs(3857, 4326, always_xy=True)


def _interp1d(sample_x, xp, fp):
    return np.interp(x=sample_x, xp=xp, fp=fp)


def _intervals(track, step: timedelta) -> np.ndarray:
    intervals = np.arange(
        start=track["time"][0],
        stop=track["time"][-1] + int(step.total_seconds()),
        step=int(step.total_seconds()),
    ).astype(int)
    assert len(intervals) >= 1
    return intervals


def np_interp_linear(track, key, intervals):
    assert key not in ("lon", "lat"), "positions require projected resampling"
    assert len(track["time"]) == len(track[key])
    return _interp1d(
        intervals.astype(int), track["time"].astype(int), track[key].astype(float)
    )


def _interp_position_linear(
    track, intervals, fwd_trans=_TO_MERCATOR, back_trans=_FROM_MERCATOR
):
    """linear resampling of positions in projected space.
    single vectorized transform per track
    """
    x, y = fwd_trans.transform(track["lon"], track["lat"])
    t = track["time"].astype(int)
    samples = intervals.astype(int)
    xi = _interp1d(samples, t, x.astype(float))
    yi = _interp1d(samples, t, y.astype(float))
    return back_trans.transform(xi, yi)


def interp_time(tracks, step: timedelta = timedelta(minutes=10)):
    """interpolation on vessel trajectory at a regular time interval.
    positions are resampled linearly in projected space (EPSG:3857),
    other dynamic values are resampled linearly over time

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

    """
    for track in tracks:
        if track["time"].size <= 1:
            warnings.warn("cannot interpolate track of length 1, skipping...")
            continue

        intervals = _intervals(track, step)

        itr = dict(
            **{k: track[k] for k in track["static"]},
            time=intervals,
            static=track["static"],
            dynamic=track["dynamic"],
        )
        if "lon" in track["dynamic"] or "lat" in track["dynamic"]:
            itr["lon"], itr["lat"] = _interp_position_linear(track, intervals)
        for key in track["dynamic"]:
            if key == "time" or key in itr:
                continue
            itr[key] = np_interp_linear(track, key, intervals)
        yield itr

    return


def geo_interp_time(tracks, step=timedelta(minutes=10), original_crs=4269):
    """Geometric interpolation on vessel trajectory, assumes default EPSG:4269

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

    """
    new_crs = 3857
    fwd_trans = Transformer.from_crs(original_crs, new_crs, always_xy=True)
    back_trans = Transformer.from_crs(new_crs, original_crs, always_xy=True)
    geod = Geod(ellps="WGS84")
    for track in tracks:
        if track["time"].size <= 1:
            warnings.warn("cannot interpolate track of length 1, skipping...")
            continue

        intervals = _intervals(track, step)

        itr = dict(
            **{k: track[k] for k in track["static"]},
            time=intervals,
            static=track["static"],
            dynamic=track["dynamic"],
        )
        if "lat" in track["dynamic"]:
            itr["lon"], itr["lat"] = _interp_position_linear(
                track, intervals, fwd_trans, back_trans
            )
            if "cog" in track["dynamic"]:
                courses, _, _ = geod.inv(
                    itr["lon"][:-1], itr["lat"][:-1], itr["lon"][1:], itr["lat"][1:]
                )
                itr["cog"] = np.append(courses, track["cog"][-1])
        for key in track["dynamic"]:
            if key == "time" or key in itr:
                continue
            itr[key] = np_interp_linear(track, key, intervals)

        yield itr

    return


def interp_spacing(spacing: int, tracks, crs=4269):
    """resample vessel trajectory at a regular distance interval

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
    """
    crs2 = 3857
    transformer = Transformer.from_crs(crs, crs2, always_xy=True)
    inv_transformer = Transformer.from_crs(crs2, crs, always_xy=True)
    geod = Geod(ellps="WGS84")

    for track in tracks:
        if track["time"].size <= 1:
            warnings.warn("cannot interpolate track of length 1, skipping...")
            continue

        # respace the coordinates in projected space
        x, y = transformer.transform(track["lon"], track["lat"])
        if len(x) == 1:
            continue
        xd = np.diff(x)
        yd = np.diff(y)
        dist = np.sqrt(xd**2 + yd**2)
        u = np.cumsum(dist)
        u = np.hstack([[0], u])
        total_dist = u[-1]
        if total_dist <= spacing:
            continue
        t = np.hstack([np.arange(0, total_dist, spacing), [total_dist]])
        xi = _interp1d(t, u, x)
        yi = _interp1d(t, u, y)
        track["lon"], track["lat"] = inv_transformer.transform(xi, yi)
        courses, _, _ = geod.inv(
            track["lon"][:-1], track["lat"][:-1], track["lon"][1:], track["lat"][1:]
        )
        if "cog" in track:
            track["cog"] = np.append(courses, track["cog"][-1])
        for k in track["dynamic"]:
            if k == "lon" or k == "lat" or k == "cog":
                continue
            track[k] = _interp1d(t, u, track[k])

        yield track


def cubic_spline(times, values, intervals):
    try:
        unique_times, unique_indices = np.unique(times, return_index=True)
        unique_values = values[unique_indices]

        assert len(unique_times) == len(unique_values)

        if not np.all(np.diff(unique_times) > 0):
            warnings.warn(
                "time values are not strictly increasing after removing "
                f"duplicates: {unique_times}"
            )
            return None

        if len(unique_times) < 2:
            warnings.warn("not enough unique time points to fit a spline")
            return None

        cs = CubicSpline(x=unique_times, y=unique_values)

        return cs(intervals)

    except Exception as e:
        warnings.warn(f"error in cubic spline: {e}. time order: {times}")
        raise


def _interp_position_spline(
    track, intervals, fwd_trans=_TO_MERCATOR, back_trans=_FROM_MERCATOR
):
    """cubic spline resampling of positions in projected space"""
    x, y = fwd_trans.transform(track["lon"], track["lat"])
    xi = cubic_spline(track["time"], x, intervals)
    yi = cubic_spline(track["time"], y, intervals)
    if xi is None or yi is None:
        return None, None
    return back_trans.transform(xi, yi)


def interp_cubic_spline(tracks, step: timedelta = timedelta(minutes=10)):
    """Cubic spline interpolation on vessel trajectory.
    positions are splined in projected space (EPSG:3857), other dynamic
    values are splined over time

    args:
        tracks (dict)
            messages sorted by mmsi then time.
            uses mmsi as key with columns: time lon lat cog sog name .. etc
        step (datetime.timedelta)
            interpolation interval

    returns:
        dictionary of interpolated tracks

    """

    for track in tracks:
        if track["time"].size <= 1:
            warnings.warn("cannot interpolate track of length 1, skipping...")
            continue

        # Sort time and dynamic data by time
        sorted_indices = np.argsort(track["time"])

        for key in track["dynamic"]:
            track[key] = track[key][sorted_indices]

        intervals = _intervals(track, step)

        itr = dict(
            **{k: track[k] for k in track["static"]},
            time=intervals,
            static=track["static"],
            dynamic=track["dynamic"],
        )
        if "lon" in track["dynamic"] or "lat" in track["dynamic"]:
            itr["lon"], itr["lat"] = _interp_position_spline(track, intervals)
        for key in track["dynamic"]:
            if key == "time" or key in itr:
                continue
            itr[key] = cubic_spline(track["time"], track[key], intervals)
        yield itr

    return

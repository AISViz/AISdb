from aisdb import dt_2_epoch
import aisdb

def test_track_interpolation():
    import numpy as np
    from datetime import timedelta, datetime

    y1, x1 = -66.84683, -61.10595523571155
    y2, x2 = -66.83036, -61.11595523571155
    y3, x3 = -66.82036, -61.12595523571155
    t1 = dt_2_epoch(datetime(2021, 1, 1, 1))
    t2 = dt_2_epoch(datetime(2021, 1, 1, 2))
    t3 = dt_2_epoch(datetime(2021, 1, 1, 3))

    # creating a sample track
    tracks_short = [
        dict(
            lon=np.array([x1, x2, x3]),
            lat=np.array([y1, y2, y3]),
            time=np.array([t1, t2, t3]),
            dynamic=set(['lon', 'lat', 'time']),
            static=set()
        )
    ]

    tracks__ = aisdb.interp.interp_time(tracks_short, timedelta(minutes=10))
    tracks__ = aisdb.interp.interp_spacing(spacing=1000, tracks=tracks__)
    for tr in tracks__:
        print(tr)
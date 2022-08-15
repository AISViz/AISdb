import os
from datetime import datetime, timedelta

import numpy as np

from aisdb import sqlfcn, sqlfcn_callbacks

y, x = 44., -63.
start = datetime(2021, 5, 1)
kwargs = dict(
    start=start,
    end=start + timedelta(days=7),
    xmin=x - 5,
    xmax=x + 5,
    ymin=y - 5,
    ymax=y + 5,
)


def test_dynamic(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_sqlfcn_dynamic.db')
    month = "202105"
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    txt = sqlfcn._dynamic(dbpath=dbpath,
                          month=month,
                          callback=callback,
                          **kwargs)
    print(txt)


def test_static(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_sqlfcn_static.db')
    month = "202105"
    txt = sqlfcn._static(dbpath=dbpath, month=month)
    print(txt)


def test_leftjoin():
    month = "202105"
    txt = sqlfcn._leftjoin(month=month)
    print(txt)


def test_crawl(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_sqlfcn_crawl.db')
    months = ['202105']
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    txt = sqlfcn.crawl_dynamic_static(dbpath=dbpath,
                                      months=months,
                                      callback=callback,
                                      **kwargs)
    print(txt)
    txt = sqlfcn.crawl_dynamic(dbpath=dbpath,
                               months=months,
                               callback=callback,
                               **kwargs)
    print(txt)


def test_callbacks(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_sqlfcn_callbacks.db')
    months = ['202105']
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    for callback in [
            sqlfcn_callbacks.in_bbox,
            sqlfcn_callbacks.in_bbox_time,
            sqlfcn_callbacks.in_bbox_time_validmmsi,
            sqlfcn_callbacks.in_time_bbox,
            sqlfcn_callbacks.in_time_bbox_hasmmsi,
            sqlfcn_callbacks.in_time_bbox_inmmsi,
            sqlfcn_callbacks.in_time_bbox_validmmsi,
            sqlfcn_callbacks.in_time_mmsi,
            sqlfcn_callbacks.in_timerange,
            sqlfcn_callbacks.in_timerange_hasmmsi,
            sqlfcn_callbacks.in_timerange_inmmsi,
            sqlfcn_callbacks.in_timerange_validmmsi,
    ]:
        box_x = np.random.random(2) * 360 - 180
        box_y = np.random.random(2) * 180 - 90
        kwargs = dict(
            start=start,
            end=start + timedelta(days=7),
            xmin=box_x[0],
            xmax=box_x[1],
            ymin=min(box_y),
            ymax=max(box_y),
        )
        txt = sqlfcn.crawl_dynamic_static(dbpath=dbpath,
                                          months=months,
                                          callback=callback,
                                          mmsi=316000000,
                                          mmsis=[316000000],
                                          **kwargs)
        print(txt)

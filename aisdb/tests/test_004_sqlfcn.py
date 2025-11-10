import os
from datetime import datetime, timedelta

import numpy as np

from aisdb import sqlfcn, sqlfcn_callbacks

y, x = 44., -63.
start = datetime(2021, 5, 1)
kwargs = dict(start=start, end=start + timedelta(days=7), xmin=x - 5, xmax=x + 5, ymin=y - 5, ymax=y + 5, )


def test_dynamic(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_dynamic.db")
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    txt = sqlfcn._dynamic(dbpath=dbpath, callback=callback, dbtype="postgresql", **kwargs)
    print(txt)


def test_static(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_static.db")
    txt = sqlfcn._static(dbpath=dbpath, dbtype="postgresql")
    print(txt)


def test_leftjoin():
    txt = sqlfcn._leftjoin(dbtype="postgresql")
    print(txt)


def test_crawl(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_crawl.db")
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    txt = sqlfcn.crawl_dynamic_static(dbpath=dbpath, callback=callback, dbtype="postgresql", **kwargs)
    print(txt)
    txt = sqlfcn.crawl_dynamic(dbpath=dbpath, callback=callback, dbtype="postgresql", **kwargs)
    print(txt)


def test_callbacks(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_callbacks.db")
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    for callback in [sqlfcn_callbacks.in_bbox_geom, sqlfcn_callbacks.in_bbox_time, sqlfcn_callbacks.in_bbox_time_validmmsi,
        sqlfcn_callbacks.in_time_bbox, sqlfcn_callbacks.in_time_bbox_hasmmsi, sqlfcn_callbacks.in_time_bbox_inmmsi,
        sqlfcn_callbacks.in_time_bbox_validmmsi, sqlfcn_callbacks.in_time_mmsi, sqlfcn_callbacks.in_timerange,
        sqlfcn_callbacks.in_timerange_hasmmsi, sqlfcn_callbacks.in_timerange_inmmsi,
        sqlfcn_callbacks.in_timerange_validmmsi, ]:
        box_x = sorted(np.random.random(2) * 360 - 180)
        box_y = sorted(np.random.random(2) * 180 - 90)
        kwargs = dict(start=start, end=start + timedelta(days=7), xmin=box_x[0], xmax=box_x[1], ymin=min(box_y),
            ymax=max(box_y), )
        txt = sqlfcn.crawl_dynamic_static(dbpath=dbpath, callback=callback, dbtype="postgresql", mmsi=316000000,
                                          mmsis=[316000000], **kwargs)
        print(txt)

import os
from datetime import datetime, timedelta

import numpy as np

from aisdb import sqlfcn, sqlfcn_callbacks

y, x = 44., -63.
start = datetime(2021, 5, 1)
kwargs = dict(
    start=start,
    end=start + timedelta(days=7),
    xmin=x - 5, xmax=x + 5,
    ymin=y - 5, ymax=y + 5
)
dbtype = 'postgresql'
month = 'global'

def test_dynamic_postgres(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_dynamic.db")
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi_geom
    txt = sqlfcn._dynamic(dbpath=dbpath, callback=callback, dbtype=dbtype, **kwargs)
    print("\n--- test_dynamic_postgres ---\n", txt)

def test_static_postgres(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_static.db")
    txt = sqlfcn._static(dbpath=dbpath, dbtype=dbtype)
    print("\n--- test_static_postgres ---\n", txt)

def test_leftjoin_postgres(tmpdir):
    txt = sqlfcn._leftjoin(dbtype=dbtype)
    print("\n--- test_leftjoin_postgres ---\n", txt)


def test_crawl_postgres(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_crawl.db")
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi_geom
    txt1 = sqlfcn.crawl_dynamic_static(dbpath=dbpath, callback=callback, dbtype=dbtype, **kwargs)
    print("\n--- crawl_dynamic_static_postgres ---\n", txt1)

    txt2 = sqlfcn.crawl_dynamic(dbpath=dbpath, callback=callback, dbtype=dbtype, **kwargs)
    print("\n--- crawl_dynamic_postgres ---\n", txt2)


def test_all_callbacks_postgres(tmpdir):
    dbpath = os.path.join(tmpdir, "test_sqlfcn_callbacks.db")
    callbacks = [
        sqlfcn_callbacks.in_bbox_geom,
        sqlfcn_callbacks.in_bbox_time_geom,
        sqlfcn_callbacks.in_bbox_time_validmmsi_geom,
        sqlfcn_callbacks.in_time_bbox_geom,
        sqlfcn_callbacks.in_time_bbox_hasmmsi_geom,
        sqlfcn_callbacks.in_time_bbox_inmmsi_geom,
        sqlfcn_callbacks.in_time_bbox_validmmsi_geom,
        sqlfcn_callbacks.in_time_mmsi,
        sqlfcn_callbacks.in_timerange,
        sqlfcn_callbacks.in_timerange_hasmmsi,
        sqlfcn_callbacks.in_timerange_inmmsi,
        sqlfcn_callbacks.in_timerange_validmmsi,
    ]

    for cb in callbacks:
        box_x = sorted(np.random.random(2) * 360 - 180)
        box_y = sorted(np.random.random(2) * 180 - 90)
        test_kwargs = dict(
            start=start,
            end=start + timedelta(days=7),
            xmin=box_x[0],
            xmax=box_x[1],
            ymin=min(box_y),
            ymax=max(box_y),
            mmsi=316000000,
            mmsis=[316000000]
        )
        try:
            txt = sqlfcn.crawl_dynamic_static(dbpath=dbpath, callback=cb, dbtype=dbtype, **test_kwargs)
            print(f"\n--- Callback: {cb.__name__} ---\n{txt}")
        except Exception as e:
            print(f"\n[ERROR] Callback: {cb.__name__} raised {e}")
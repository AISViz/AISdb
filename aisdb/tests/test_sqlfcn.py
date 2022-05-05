from datetime import datetime, timedelta

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


def test_dynamic():
    month = "202105"
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    txt = sqlfcn._dynamic(month, callback, **kwargs)
    print(txt)


def test_static():
    month = "202105"
    txt = sqlfcn._static(month=month)
    print(txt)


def test_leftjoin():
    month = "202105"
    txt = sqlfcn._leftjoin(month=month)
    print(txt)


def test_crawl():
    months = ['202105']
    callback = sqlfcn_callbacks.in_time_bbox_validmmsi
    txt = sqlfcn.crawl_dynamic_static(months, callback, **kwargs)
    print(txt)
    txt = sqlfcn.crawl_dynamic(months, callback, **kwargs)
    print(txt)

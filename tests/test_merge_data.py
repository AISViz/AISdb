from datetime import datetime

from aisdb.database.qrygen import DBQuery
from aisdb.track_gen import trackgen
from aisdb.webdata.merge_data import (
    merge_layers,
    merge_tracks_bathymetry,
    merge_tracks_hullgeom,
    merge_tracks_shoredist,
)
from aisdb.database.lambdas import in_bbox_time


def prepare_qry():
    start = datetime(2021, 11, 1)
    end = datetime(2021, 11, 2)

    rowgen = DBQuery(
        start=start,
        end=end,
        xmin=-60,
        xmax=-45,
        ymin=40,
        ymax=60,
        callback=in_bbox_time,
    ).gen_qry()

    return rowgen


def test_merge_shoredist():
    merged = merge_tracks_shoredist(trackgen(prepare_qry()))
    test = next(merged)
    print(test)


def test_merge_bathymetry():
    merged = merge_tracks_bathymetry(trackgen(prepare_qry()))
    test = next(merged)
    print(test)


def test_merge_hullgeom():
    merged = merge_tracks_hullgeom(trackgen(prepare_qry()))
    test = next(merged)
    print(test)


def test_merge_layers_all():
    merged = merge_layers(trackgen(prepare_qry()))
    test = next(merged)
    print(test)

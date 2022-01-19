from datetime import datetime

from aisdb.database.dbqry import DBQuery
from aisdb.track_gen import TrackGen
from aisdb.webdata.merge_data import (
    merge_layers,
    merge_tracks_bathymetry,
    merge_tracks_hullgeom,
    merge_tracks_shoredist,
)
from aisdb.database.sqlfcn_callbacks import in_bbox_time
from aisdb.gis import ZoneGeom, Domain
from tests.create_testing_data import sample_gulfstlawrence_zonegeometry

import numpy as np


def prepare_qry():
    z1 = sample_gulfstlawrence_zonegeometry
    domain = Domain('gulf domain', geoms={'z1': z1}, cache=False)

    start = datetime(2021, 11, 1)
    end = datetime(2021, 11, 7)

    rowgen = DBQuery(
        start=start,
        end=end,
        xmin=domain.minX,
        xmax=domain.maxX,
        ymin=domain.minY,
        ymax=domain.maxY,
        callback=in_bbox_time,
    ).gen_qry()

    return rowgen


def test_merge_shoredist():
    merged = merge_tracks_shoredist(TrackGen(prepare_qry()))
    test = next(merged)
    print(test)


def test_merge_bathymetry():
    merged = merge_tracks_bathymetry(TrackGen(prepare_qry()))
    test = next(merged)
    print(test)


def test_merge_hullgeom():
    merged = merge_tracks_hullgeom(TrackGen(prepare_qry()))
    test = next(merged)
    print(test)


def test_merge_layers_all():
    merged = merge_layers(TrackGen(prepare_qry()))
    test = next(merged)
    print(test)

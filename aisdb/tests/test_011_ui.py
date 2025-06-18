import os
from datetime import datetime, timedelta

from shapely.geometry import Polygon

import aisdb
from aisdb import PostgresDBConn, Domain, DBQuery, sqlfcn_callbacks
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox
from aisdb.web_interface import serialize_zone_json, serialize_track_json


def _add_track_color(tracks):
    for i, track in enumerate(tracks):
        if i % 2 == 0:
            track["color"] = "#ffffff"
            track["marinetraffic_info"] = {}
        yield track


def test_ui_serialize():
    start = datetime(2023, 7, 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": z1}])

    zone_bytes = serialize_zone_json("z1", domain.zones["z1"])
    assert isinstance(zone_bytes, bytes)

    conn_str = (
        f"postgresql://{os.environ['pguser']}:{os.environ['pgpass']}@"
        f"{os.environ['pghost']}:5432/{os.environ['pguser']}"
    )

    with PostgresDBConn(conn_str) as dbconn:
        rowgen = DBQuery(dbconn=dbconn, start=start, end=end, **domain.boundary,
                         callback=sqlfcn_callbacks.in_timerange).gen_qry(reaggregate_static=True)
        tracks = aisdb.TrackGen(rowgen, decimate=True)
        tracks = list(_add_track_color(tracks))

        for track in tracks:
            assert isinstance(serialize_track_json(track)[0], bytes)

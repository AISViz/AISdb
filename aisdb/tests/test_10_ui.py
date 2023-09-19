import os
from datetime import datetime, timedelta

from shapely.geometry import Polygon

import aisdb
from aisdb import SQLiteDBConn, Domain, DBQuery, sqlfcn_callbacks
from aisdb.web_interface import serialize_zone_json, serialize_track_json, visualize
from aisdb.tests.create_testing_data import (
    sample_database_file,
    sample_gulfstlawrence_bbox,
)


def _add_track_color(tracks):
    i = 0
    for track in tracks:
        if i % 2 == 0:
            track['color'] = '#ffffff'
            track['marinetraffic_info'] = {}
        i += 1
        yield track


def test_ui_serialize(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_ui_serialize.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])

    zone_bytes = serialize_zone_json('z1', domain.zones['z1'])
    assert isinstance(zone_bytes, bytes)

    with SQLiteDBConn(testdbpath) as dbconn:
        rowgen = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_timerange,
        ).gen_qry(reaggregate_static=True)
        tracks = aisdb.TrackGen(rowgen, decimate=True)
        tracks = _add_track_color(tracks)
        tracks = list(tracks)

        for track in tracks:
            assert isinstance(serialize_track_json(track)[0], bytes)
        #visualize(tracks, visualearth=True, open_browser=False)
        #visualize(tracks, visualearth=False, open_browser=False)

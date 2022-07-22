import os

import numpy as np
from shapely.geometry import Polygon

from aisdb.gis import Domain
from aisdb.database.create_tables import (
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)
from aisdb import decode_msgs, DBConn, aggregate_static_msgs


def sample_dynamictable_insertdata(*, db, dbpath):
    #db = DBConn(dbpath=testdbpath)
    assert isinstance(db, DBConn)
    sqlite_createtable_staticreport(db, month="200001", dbpath=dbpath)
    sqlite_createtable_dynamicreport(db, month="200001", dbpath=dbpath)
    db.cur.execute(
        'INSERT OR IGNORE INTO ais_200001_dynamic (mmsi, time, longitude, latitude, cog, sog) VALUES (000000001, 946702800, -60.994833, 47.434647238127695, -1, -1)'
    )
    db.cur.execute(
        'INSERT OR IGNORE INTO ais_200001_dynamic (mmsi, time, longitude, latitude, cog, sog) VALUES (000000001, 946702820, -60.994833, 47.434647238127695, -1, -1)'
    )
    db.cur.execute(
        'INSERT OR IGNORE INTO ais_200001_dynamic (mmsi, time, longitude, latitude, cog, sog) VALUES (000000001, 946702840, -60.994833, 47.434647238127695, -1, -1)'
    )
    db.conn.commit()


def sample_random_polygon(xscale=20, yscale=20):
    vertices = 6

    x, y = [0, 0, 0], [0, 0, 0]
    while not Polygon(zip(x, y)).is_valid:
        x = (np.random.random(vertices) * xscale) + (350 *
                                                     (np.random.random() - .5))
        y = (np.random.random(vertices) * yscale) + (170 *
                                                     (np.random.random() - .5))

    return x, y


def sample_gulfstlawrence_bbox():
    gulfstlawrence_bbox_xy = np.array([
        (-71.64440346704974, 43.18445256159233),
        (-71.2966623933639, 52.344721551389526),
        (-51.2146153880073, 51.68484191466307),
        (-50.345262703792734, 42.95158299927571),
        (-71.64440346704974, 43.18445256159233),
    ])
    return gulfstlawrence_bbox_xy.T


def random_polygons_domain(count=10):
    return Domain('testdomain',
                  [{
                      'name': 'random',
                      'geometry': Polygon(zip(*sample_random_polygon()))
                  } for _ in range(count)])


def sample_database_file(dbpath):
    ''' test data for date 2021-11-01 '''
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    months = ["202111"]
    with DBConn(dbpath=dbpath) as db:
        decode_msgs(
            db=db,
            filepaths=[datapath],
            dbpath=dbpath,
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )
        aggregate_static_msgs(db, months)
    return months

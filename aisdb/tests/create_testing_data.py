import os

import numpy as np
from shapely.geometry import Polygon

from aisdb.gis import Domain
from aisdb.database.create_tables import (
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)
from aisdb import decode_msgs, DBConn, aggregate_static_msgs


def sample_dynamictable_insertdata(*, dbconn, dbpath):
    #db = DBConn()
    assert isinstance(dbconn, DBConn)
    sqlite_createtable_staticreport(dbconn, month="200001", dbpath=dbpath)
    sqlite_createtable_dynamicreport(dbconn, month="200001", dbpath=dbpath)
    dbconn.execute(
        'INSERT OR IGNORE INTO ais_200001_dynamic (mmsi, time, longitude, latitude, cog, sog) VALUES (000000001, 946702800, -60.994833, 47.434647238127695, -1, -1)'
    )
    dbconn.execute(
        'INSERT OR IGNORE INTO ais_200001_dynamic (mmsi, time, longitude, latitude, cog, sog) VALUES (000000001, 946702820, -60.994833, 47.434647238127695, -1, -1)'
    )
    dbconn.execute(
        'INSERT OR IGNORE INTO ais_200001_dynamic (mmsi, time, longitude, latitude, cog, sog) VALUES (000000001, 946702840, -60.994833, 47.434647238127695, -1, -1)'
    )
    dbconn.commit()


def sample_random_polygon(xscale=10, yscale=10):
    vertices = 6

    x, y = [0, 0, 0], [0, 0, 0]
    while not Polygon(zip(x, y)).is_valid:
        x = (np.random.random(vertices) * xscale) + (180 *
                                                     (np.random.random() - .5))
        y = (np.random.random(vertices) * yscale) + (90 *
                                                     (np.random.random() - .5))

    return x, y


def sample_invalid_polygon(xscale=10, yscale=10):
    vertices = 6

    x, y = [0, 0, 0], [0, 0, 0]
    while not Polygon(zip(x, y)).is_valid:
        x = (np.random.random(vertices) * xscale) + (3600 *
                                                     (np.random.random() - .5))
        y = (np.random.random(vertices) * yscale) + (1800 *
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
                      'name': f'random_{i:03}',
                      'geometry': Polygon(zip(*sample_random_polygon()))
                  } for i in range(count)])


def sample_database_file(dbpath):
    ''' test data for date 2021-11-01 '''
    datapath_csv = os.path.join(os.path.dirname(__file__),
                                'test_data_20210701.csv')
    # no static data in nm4
    datapath_nm4 = os.path.join(os.path.dirname(__file__),
                                'test_data_20211101.nm4')
    months = ["202107", "202111"]
    with DBConn() as dbconn:
        dbconn._attach(dbpath)
        decode_msgs(
            dbconn=dbconn,
            filepaths=[datapath_csv, datapath_nm4],
            dbpath=dbpath,
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )
        aggregate_static_msgs(dbconn, months[:1])
        dbconn.commit()
    return months

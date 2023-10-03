import os

import numpy as np
from shapely.geometry import Polygon

from aisdb.gis import Domain
from aisdb.database.create_tables import (
    sql_createtable_dynamic,
    sql_createtable_static,
)
from aisdb import decode_msgs, DBConn

postgres_test_conn = dict(hostaddr='fc00::17',
                          user='postgres',
                          port=5431,
                          password='devel')


def sample_dynamictable_insertdata(*, dbconn):
    #db = DBConn()
    assert isinstance(dbconn, DBConn)
    dbconn.execute(sql_createtable_static.format(month="200001"))
    dbconn.execute(sql_createtable_dynamic.format(month="200001"))
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


def sample_random_polygon(xscale=50, yscale=50):
    vertices = 5

    x, y = [0, 0, 0], [0, 0, 0]
    while not Polygon(zip(x, y)).is_valid:
        x = np.random.random(vertices) * xscale
        x += np.random.randint(360 - xscale)
        x -= 180
        y = np.random.random(vertices) * yscale
        y += np.random.randint(180 - yscale)
        y -= 90
        assert min(x) >= -180, min(x)
        assert max(x) <= 180, max(x)
        assert min(y) >= -90, min(y)
        assert max(y) <= 90, max(y)

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
    assert os.path.isdir(os.path.join(os.path.dirname(__file__), 'testdata'))
    datapath_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                'test_data_20210701.csv')
    # no static data in nm4
    datapath_nm4 = os.path.join(os.path.dirname(__file__), 'testdata',
                                'test_data_20211101.nm4')
    months = ["202107", "202111"]
    with DBConn(dbpath) as dbconn:
        decode_msgs(
            dbconn=dbconn,
            filepaths=[datapath_csv, datapath_nm4],
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )
        dbconn.aggregate_static_msgs(months)
        dbconn.commit()
    return months

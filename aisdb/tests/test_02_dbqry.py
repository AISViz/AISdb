import os
import warnings
from datetime import datetime, timedelta

import pytest
from shapely.geometry import Polygon

from aisdb import DBConn, DBQuery, sqlfcn_callbacks, Domain, sqlfcn
from aisdb.database.dbqry import DBQuery_async

from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)
from aisdb.tests.create_testing_data import (
    sample_database_file,
    sample_dynamictable_insertdata,
    sample_gulfstlawrence_bbox,
)


def test_query_emptytable(tmpdir):
    warnings.filterwarnings('error')
    dbpath = os.path.join(tmpdir, 'test_query_emptytable.db')
    with DBConn() as dbconn:
        q = DBQuery(
            dbconn=dbconn,
            dbpath=dbpath,
            start=datetime(2021, 1, 1),
            end=datetime(2021, 1, 7),
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        sqlite_createtable_dynamicreport(dbconn, month='202101', dbpath=dbpath)
        rows = q.gen_qry()
        try:
            next(rows)
        except SyntaxError:
            pass
        except UserWarning:
            pass
        except Exception as err:
            raise err


def test_prepare_qry_domain(tmpdir):

    testdbpath = os.path.join(tmpdir, 'test_prepare_qry_domain.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])
    with DBConn() as aisdatabase:
        rowgen = DBQuery(
            dbconn=aisdatabase,
            dbpath=testdbpath,
            start=start,
            end=end,
            xmin=domain.minX,
            xmax=domain.maxX,
            ymin=domain.minY,
            ymax=domain.maxY,
            callback=sqlfcn_callbacks.in_timerange,
        ).gen_qry()
        try:
            next(rowgen)
        except SyntaxError:
            pass
        except Exception as err:
            raise err


def test_sql_query_strings(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_sql_query_strings.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])
    with DBConn() as aisdatabase:
        for callback in [
                sqlfcn_callbacks.in_timerange,
                sqlfcn_callbacks.in_timerange_validmmsi,
                sqlfcn_callbacks.in_timerange_hasmmsi,
                sqlfcn_callbacks.in_time_bbox_inmmsi
        ]:
            rowgen = DBQuery(
                dbconn=aisdatabase,
                dbpath=testdbpath,
                start=start,
                end=end,
                xmin=domain.maxX,
                xmax=domain.minX,
                ymin=domain.minY,
                ymax=domain.maxY,
                callback=callback,
                mmsi=316000000,
                mmsis=[316000000, 316000001],
            ).gen_qry(fcn=sqlfcn.crawl_dynamic_static)
            try:
                next(rowgen)
            except SyntaxError:
                pass
            except Exception as err:
                raise err
    return


@pytest.mark.asyncio
async def test_query_async(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_query_async.db')
    # create db synchronously
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    q = DBQuery_async(
        dbpath=testdbpath,
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    async for rows in q.gen_qry():
        print(rows)

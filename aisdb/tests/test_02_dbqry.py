import os
import warnings
from datetime import datetime, timedelta

from shapely.geometry import Polygon

from aisdb import DBConn, DBQuery, sqlfcn_callbacks, Domain, sqlfcn

from aisdb.database.create_tables import sqlite_createtable_dynamicreport
from aisdb.tests.create_testing_data import (
    sample_database_file,
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
        rows = q.gen_qry(reaggregate_static=True)
        try:
            next(rows)
        except StopIteration:
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
            **domain.boundary,
            callback=sqlfcn_callbacks.in_timerange,
        ).gen_qry(reaggregate_static=True)
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
                sqlfcn_callbacks.in_bbox,
                sqlfcn_callbacks.in_bbox_time,
                sqlfcn_callbacks.in_bbox_time_validmmsi,
                sqlfcn_callbacks.in_time_bbox_inmmsi,
                sqlfcn_callbacks.in_timerange,
                sqlfcn_callbacks.in_timerange_hasmmsi,
                sqlfcn_callbacks.in_timerange_validmmsi,
        ]:
            rowgen = DBQuery(
                dbconn=aisdatabase,
                dbpath=testdbpath,
                start=start,
                end=end,
                **domain.boundary,
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

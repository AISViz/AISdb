import os
from datetime import datetime

import pytest
from shapely.geometry import Polygon

from aisdb import DBConn, DBQuery, sqlfcn_callbacks, Domain
from aisdb.database.dbconn import DBConn_async
from aisdb.database.dbqry import DBQuery_async

from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)
from aisdb.tests.create_testing_data import (
    sample_dynamictable_insertdata,
    sample_gulfstlawrence_bbox,
)

start = datetime(2020, 9, 1)
end = datetime(2020, 10, 1)


def test_query_emptytable(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_query_emptytable.db')
    with DBConn(dbpath=dbpath) as db:
        q = DBQuery(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        _rows = q.gen_qry(dbpath=dbpath)


def test_prepare_qry_domain(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_prepare_qry_domain.db')
    #aisdatabase = DBConn(dbpath=testdbpath)
    with DBConn(dbpath=testdbpath) as aisdatabase:
        sqlite_createtable_staticreport(aisdatabase,
                                        month="200001",
                                        dbpath=testdbpath)
        sqlite_createtable_dynamicreport(aisdatabase,
                                         month="200001",
                                         dbpath=testdbpath)
        sample_dynamictable_insertdata(db=aisdatabase, dbpath=testdbpath)

        z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
        domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])

        start = datetime(2000, 1, 1)
        end = datetime(2000, 2, 1)

        rowgen = DBQuery(
            db=aisdatabase,
            start=start,
            end=end,
            xmin=domain.minX,
            xmax=domain.maxX,
            ymin=domain.minY,
            ymax=domain.maxY,
            callback=sqlfcn_callbacks.in_timerange,
        ).gen_qry(dbpath=testdbpath)

        return rowgen


@pytest.mark.asyncio
async def test_query_async(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_query_async.db')
    #aisdatabase = DBConn_async(dbpath=testdbpath)
    # create db synchronously
    with DBConn(dbpath=testdbpath) as aisdatabase:
        sqlite_createtable_staticreport(db=aisdatabase,
                                        month="202009",
                                        dbpath=testdbpath)
        sqlite_createtable_dynamicreport(aisdatabase,
                                         month="202009",
                                         dbpath=testdbpath)
        aggregate_static_msgs(aisdatabase, ["202009"])
    async with DBConn_async(dbpath=testdbpath) as aisdatabase:
        q = DBQuery_async(
            db=aisdatabase,
            dbpath=testdbpath,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        async for rows in q.gen_qry(dbpath=testdbpath):
            print(rows)

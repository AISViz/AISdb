import os
from datetime import datetime

import pytest
from shapely.geometry import Polygon

from aisdb import DBConn, DBQuery, sqlfcn_callbacks, Domain

from aisdb.database.create_tables import (
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
    db = os.path.join(tmpdir, 'test_dbqry.db')
    q = DBQuery(
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    _rows = q.gen_qry(dbpath=db)


def test_prepare_qry_domain(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_dbqry.db')
    aisdatabase = DBConn(dbpath=testdbpath)
    sqlite_createtable_staticreport(aisdatabase.cur, month="200001")
    sqlite_createtable_dynamicreport(aisdatabase.cur, month="200001")
    sample_dynamictable_insertdata(testdbpath)

    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain('gulf domain', zones=[{'name': 'z1', 'geometry': z1}])

    start = datetime(2000, 1, 1)
    end = datetime(2000, 2, 1)

    rowgen = DBQuery(
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
    db = os.path.join(tmpdir, 'test_dbqry.db')
    aisdatabase = DBConn(dbpath=db)
    sqlite_createtable_staticreport(aisdatabase.cur, month="202009")
    sqlite_createtable_dynamicreport(aisdatabase.cur, month="202009")
    q = DBQuery(
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    async for rows in q.async_qry(dbpath=db):
        print(rows)

import os
from datetime import datetime

import pytest
from shapely.geometry import Polygon

from aisdb import DBQuery, sqlfcn_callbacks, Domain

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
    q.check_idx(dbpath=db)
    _rows = q.gen_qry(dbpath=db)


def test_prepare_qry_domain(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_dbqry.db')
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
    q = DBQuery(
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    q.check_idx(dbpath=db)
    async for rows in q.async_qry(dbpath=db):
        print(rows)

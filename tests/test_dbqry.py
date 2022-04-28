import os
from datetime import datetime

import pytest

from aisdb import DBQuery, data_dir, sqlfcn_callbacks

db = os.path.join(data_dir, 'testdb', 'test1.db')
start = datetime(2020, 9, 1)
end = datetime(2020, 10, 1)


def cleanup():
    if os.path.isfile(db):
        os.remove(db)


def test_query_emptytable():
    q = DBQuery(
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    q.check_idx(dbpath=db)
    _rows = q.gen_qry(dbpath=db)
    cleanup()


@pytest.mark.asyncio
async def test_query_async():
    q = DBQuery(
        start=start,
        end=end,
        callback=sqlfcn_callbacks.in_timerange_validmmsi,
    )
    q.check_idx(dbpath=db)
    async for rows in q.async_qry(dbpath=db):
        print(rows)

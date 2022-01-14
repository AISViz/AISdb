import os
from datetime import datetime

from aisdb.common import data_dir
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi
from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)

start = datetime(2020, 9, 1)
end = datetime(2020, 10, 1)

if not os.path.isdir(data_dir):
    os.mkdir(data_dir)

#if not os.path.isdir(os.path.join(data_dir, 'testdb')):
#    os.mkdir(os.path.join(data_dir, 'testdb'))

db = os.path.join(data_dir, 'test1.db')

if os.path.isfile(db):
    os.remove(db)

#conn, cur = aisdatabase.conn, aisdatabase.cur


def test_create_static_table():
    aisdatabase = DBConn(dbpath=db)
    sqlite_createtable_staticreport(aisdatabase.cur, month="202009")
    aisdatabase.conn.close()


def test_create_dynamic_table():

    aisdatabase = DBConn(dbpath=db)
    sqlite_createtable_dynamicreport(aisdatabase.cur, month="202009")
    aisdatabase.conn.commit()
    aisdatabase.conn.close()


def test_create_static_aggregate_table():
    aisdatabase = DBConn(dbpath=db)
    _ = sqlite_createtable_staticreport(aisdatabase.cur, "202009")
    aggregate_static_msgs(db, ["202009"])
    aisdatabase.conn.close()


def test_query_emptytable():
    #aisdatabase = DBConn(dbpath=db)
    #sqlite_createtable_staticreport(aisdatabase.cur, month="202009")
    #sqlite_createtable_dynamicreport(aisdatabase.cur, month="202009")
    #aisdatabase.conn.commit()

    dt = datetime.now()
    q = DBQuery(
        start=start,
        end=end,
        callback=in_timerange_validmmsi,
    )
    q.check_idx(dbpath=db)
    _rows = q.run_qry(dbpath=db)
    delta = datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')
    #aisdatabase.conn.close()

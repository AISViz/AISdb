import os
from datetime import datetime

from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb.database.decoder import decode_msgs
from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi
from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)

start = datetime(2020, 9, 1)
end = datetime(2020, 10, 1)


def test_create_static_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_static_table.db')
    with DBConn(dbpath=dbpath) as db:
        sqlite_createtable_staticreport(db, month="202009", dbpath=dbpath)


def test_create_dynamic_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_dynamic_table.db')
    with DBConn(dbpath=dbpath) as db:
        sqlite_createtable_dynamicreport(db, month="202009", dbpath=dbpath)


def test_create_static_aggregate_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_static_aggregate_table.db')
    testingdata_nm4 = os.path.join(os.path.dirname(__file__),
                                   'testingdata_20211101.nm4')
    testingdata_csv = os.path.join(os.path.dirname(__file__),
                                   'testingdata_20210701.csv')
    with DBConn(dbpath=dbpath) as db:
        decode_msgs([testingdata_csv, testingdata_nm4],
                    db=db,
                    dbpath=dbpath,
                    source='TESTING')
        aggregate_static_msgs(db, ["202111", "202107"])

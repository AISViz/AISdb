import os
import warnings

from aisdb.database.dbconn import DBConn
from aisdb.database.decoder import decode_msgs
from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)


def test_create_static_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_static_table.db')
    with DBConn() as dbconn:
        sqlite_createtable_staticreport(dbconn, month="202009", dbpath=dbpath)


def test_create_dynamic_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_dynamic_table.db')
    with DBConn() as dbconn:
        sqlite_createtable_dynamicreport(dbconn, month="202009", dbpath=dbpath)


def test_create_static_aggregate_table(tmpdir):
    warnings.filterwarnings('error')
    dbpath = os.path.join(tmpdir, 'test_create_static_aggregate_table.db')
    testingdata_csv = os.path.join(os.path.dirname(__file__),
                                   'test_data_20210701.csv')
    with DBConn() as dbconn:
        decode_msgs([testingdata_csv],
                    dbconn=dbconn,
                    dbpath=dbpath,
                    source='TESTING')
        aggregate_static_msgs(dbconn, ["202107"])

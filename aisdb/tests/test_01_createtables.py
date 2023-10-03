import os
import warnings

from aisdb.database.dbconn import DBConn, PostgresDBConn
from aisdb.database.decoder import decode_msgs
from aisdb.database.create_tables import (
    sql_createtable_dynamic,
    sql_createtable_static,
)
from aisdb.tests.create_testing_data import postgres_test_conn


def test_create_static_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_static_table.db')
    with DBConn(dbpath) as dbconn:
        dbconn.execute(sql_createtable_dynamic.format("202009"))


def test_create_dynamic_table(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_create_dynamic_table.db')
    with DBConn(dbpath) as dbconn:
        dbconn.execute(sql_createtable_dynamic.format("202009"))


def test_create_static_aggregate_table(tmpdir):
    warnings.filterwarnings('error')
    dbpath = os.path.join(tmpdir, 'test_create_static_aggregate_table.db')
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    with DBConn(dbpath) as dbconn:
        decode_msgs([testingdata_csv], dbconn=dbconn, source='TESTING')
        dbconn.aggregate_static_msgs(["202107"])


def test_create_from_CSV(tmpdir):
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    dbpath = os.path.join(tmpdir, 'test_create_from_CSV.db')
    with DBConn(dbpath) as dbconn:
        decode_msgs(
            dbconn=dbconn,
            filepaths=[testingdata_csv],
            source='TESTING',
            vacuum=False,
        )

        cur = dbconn.cursor()
        cur.execute(
            # need to specify datbabase name in SQL statement
            "SELECT name FROM sqlite_schema "
            "WHERE type='table' ORDER BY name;")
        rows = cur.fetchall()
        temp = [row['name'] for row in rows]
        print(temp)
        assert len(temp) == 5


def test_create_from_CSV_postgres(tmpdir):
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    with PostgresDBConn(**postgres_test_conn) as dbconn:
        decode_msgs(
            dbconn=dbconn,
            filepaths=[testingdata_csv],
            source='TESTING',
            vacuum=False,
        )
        cur = dbconn.cursor()
        cur.execute(
            # need to specify datbabase name in SQL statement
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name;")
        tables = [row["table_name"] for row in cur.fetchall()]
        assert 'ais_202107_dynamic' in tables

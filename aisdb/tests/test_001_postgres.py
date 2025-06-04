import os
from datetime import datetime, timedelta

from shapely.geometry import Polygon

from aisdb import (DBQuery, Domain, PostgresDBConn, sqlfcn, sqlfcn_callbacks, )
from aisdb.database.decoder import decode_msgs
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox
from aisdb.track_gen import TrackGen

conn_information = (f"postgresql://{os.environ['pguser']}:{os.environ['pgpass']}@"
                    f"{os.environ['pghost']}:5432/{os.environ['pguser']}")


def test_postgres():
    # keyword arguments
    with PostgresDBConn(conn_information) as dbconn:
        cur = dbconn.cursor()
        cur.execute("select * from coarsetype_ref;")
        res = cur.fetchall()
        print(res)


def test_create_from_CSV_postgres(tmpdir):
    testingdata_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    with PostgresDBConn(conn_information) as dbconn:
        decode_msgs(dbconn=dbconn, filepaths=[testingdata_csv], source="TESTING", vacuum=False, )
        cur = dbconn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name;")
        tables = [row["table_name"] for row in cur.fetchall()]
        assert "ais_202107_dynamic" in tables
        cur.execute(
            """
            DROP TABLE IF EXISTS ais_202107_dynamic CASCADE;
            DROP TABLE IF EXISTS ais_202107_static CASCADE;
            DROP TABLE IF EXISTS static_202107_aggregate CASCADE;
            """
        )


def test_create_from_CSV_postgres_timescaledb(tmpdir):
    testingdata_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    with PostgresDBConn(conn_information) as dbconn:
        decode_msgs(dbconn=dbconn, filepaths=[testingdata_csv], source="TESTING", vacuum=False,
                    skip_checksum=True, raw_insertion=True, timescaledb=True)
        cur = dbconn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name;")
        tables = [row["table_name"] for row in cur.fetchall()]
        assert "ais_202107_dynamic" in tables

        # Fetch and print indexes for the table
        cur.execute("""
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE tablename = 'ais_202107_dynamic';
        """)
        indexes = cur.fetchall()
        print(indexes)


def test_decode_1day_postgres(tmpdir):
    testingdata_nm4 = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4")
    testingdata_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    testingdata_gz = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.gz")
    testingdata_zip = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.zip")
    filepaths = [testingdata_csv, testingdata_nm4, testingdata_gz, testingdata_zip]

    with PostgresDBConn(conn_information) as dbconn:
        # dbconn.execute("TRUNCATE hashmap")
        # dbconn.commit()

        dt = datetime.now()
        decode_msgs(filepaths=filepaths, dbconn=dbconn, source="TESTING", vacuum=True, verbose=True)
        delta = datetime.now() - dt
        print(f"postgres total parse and insert time: {delta.total_seconds():.2f}s")


def test_sql_query_strings_postgres(tmpdir):
    testingdata_nm4 = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4")
    testingdata_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    testingdata_gz = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.gz")
    testingdata_zip = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.zip")
    filepaths = [testingdata_csv, testingdata_nm4, testingdata_gz, testingdata_zip]
  
    months = ["202107", "202111"]
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": z1}])
    with PostgresDBConn(conn_information) as aisdatabase:
        decode_msgs(filepaths=filepaths, dbconn=aisdatabase, source="TESTING", vacuum=True, verbose=True,
                    skip_checksum=True)
        for callback in [sqlfcn_callbacks.in_bbox, sqlfcn_callbacks.in_bbox_time,
            sqlfcn_callbacks.in_bbox_time_validmmsi, sqlfcn_callbacks.in_time_bbox_inmmsi,
            sqlfcn_callbacks.in_timerange, sqlfcn_callbacks.in_timerange_hasmmsi,
            sqlfcn_callbacks.in_timerange_validmmsi, ]:
            rowgen = DBQuery(dbconn=aisdatabase, start=start, end=end, **domain.boundary, callback=callback,
                mmsi=316000000, mmsis=[316000000, 316000001], ).gen_qry(fcn=sqlfcn.crawl_dynamic_static)
            next(rowgen)
        aisdatabase.rebuild_indexes(months[0], verbose=False)
        aisdatabase.deduplicate_dynamic_msgs(months[0], verbose=True)
        aisdatabase.deduplicate_dynamic_msgs(months[0], verbose=False)
import os
from datetime import datetime, timedelta

from shapely.geometry import Polygon

from aisdb import (DBConn, DBQuery, Domain, PostgresDBConn, sqlfcn, sqlfcn_callbacks, )
from aisdb.database.decoder import decode_msgs
from aisdb.tests.create_testing_data import (sample_database_file, sample_gulfstlawrence_bbox, )
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
        cur.execute(# need to specify database name in SQL statement
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name;")
        tables = [row["table_name"] for row in cur.fetchall()]
        assert "ais_202107_dynamic" in tables


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
    # testdbpath = os.path.join(tmpdir, "test_sql_query_strings.db")
    # months = sample_database_file(testdbpath)
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


def test_compare_sqlite_postgres_query_output(tmpdir):
    testdbpath = os.path.join(tmpdir, "test_compare_sqlite_postgres_query_output.db")
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = datetime(int(months[-1][0:4]), int(months[-1][4:6]), 1)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": z1}])

    testingdata_nm4 = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4")
    testingdata_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    testingdata_gz = os.path.join(os.path.dirname(__file__), "testdata", 'test_data_20211101.nm4.gz')
    testingdata_zip = os.path.join(os.path.dirname(__file__), 'testdata', 'test_data_20211101.nm4.zip')
    filepaths = [testingdata_csv, testingdata_nm4, testingdata_gz, testingdata_zip]

    with DBConn(testdbpath) as sqlitedb, PostgresDBConn(conn_information) as pgdb:
        decode_msgs(filepaths=filepaths, dbconn=sqlitedb, source='TESTING_SQLITE', vacuum=False, verbose=True,
                    skip_checksum=True)
        sqlitedb.commit()

        decode_msgs(filepaths=filepaths, dbconn=pgdb, source='TESTING_POSTGRES', vacuum=False, verbose=True,
                    skip_checksum=True)

        pgdb.commit()

        rowgen1 = DBQuery(dbconn=sqlitedb, start=start, end=end, **domain.boundary,
            callback=sqlfcn_callbacks.in_time_bbox_validmmsi, ).gen_qry(reaggregate_static=True)

        rowgen2 = DBQuery(dbconn=pgdb, start=start, end=end, **domain.boundary,
            callback=sqlfcn_callbacks.in_time_bbox_validmmsi, ).gen_qry(reaggregate_static=True)

        tracks1 = list(TrackGen(rowgen1, decimate=False))
        tracks2 = list(TrackGen(rowgen2, decimate=False))

    for a, b in zip(tracks1, tracks2):
        assert a['lat'] == b['lat']

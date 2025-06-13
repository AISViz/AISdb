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

# CSV ingestion to global hypertables
def test_create_from_CSV_postgres_global(tmpdir):
    testingdata_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    with PostgresDBConn(conn_information) as dbconn:
        decode_msgs(
            dbconn=dbconn,
            filepaths=[testingdata_csv],
            source="TESTING",
            vacuum=False,
            skip_checksum=True,
            raw_insertion=True,
            timescaledb=True,
        )
        cur = dbconn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        tables = [row["table_name"] for row in cur.fetchall()]
        assert "ais_global_dynamic" in tables

def test_decode_1day_postgres_global(tmpdir):
    filepaths = [
        os.path.join(os.path.dirname(__file__), "testdata", f)
        for f in ["test_data_20210701.csv", "test_data_20211101.nm4", 
                  "test_data_20211101.nm4.gz", "test_data_20211101.nm4.zip"]
    ]

    with PostgresDBConn(conn_information) as dbconn:
        dt = datetime.now()
        decode_msgs(
            filepaths=filepaths,
            dbconn=dbconn,
            source="TESTING",
            vacuum=True,
            verbose=True,
            skip_checksum=True,
            raw_insertion=True,
            timescaledb=True
        )

        delta = datetime.now() - dt
        print(f"postgres total parse and insert time: {delta.total_seconds():.2f}s")

def test_sql_query_strings_postgres_global(tmpdir):
    filepaths = [
        os.path.join(os.path.dirname(__file__), "testdata", f)
        for f in ["test_data_20210701.csv", "test_data_20211101.nm4", 
                  "test_data_20211101.nm4.gz", "test_data_20211101.nm4.zip"]
    ]

    start = datetime(2021, 7, 1)
    end = start + timedelta(weeks=4)
    z1 = Polygon(zip(*sample_gulfstlawrence_bbox()))
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": z1}])

    with PostgresDBConn(conn_information) as aisdatabase:
        decode_msgs(filepaths=filepaths, dbconn=aisdatabase, source="TESTING", vacuum=True, verbose=True,
                    skip_checksum=True, raw_insertion=True, timescaledb=True)

        for callback in [
            sqlfcn_callbacks.in_bbox, sqlfcn_callbacks.in_bbox_time,
            sqlfcn_callbacks.in_bbox_time_validmmsi, sqlfcn_callbacks.in_time_bbox_inmmsi,
            sqlfcn_callbacks.in_timerange, sqlfcn_callbacks.in_timerange_hasmmsi,
            sqlfcn_callbacks.in_timerange_validmmsi,
        ]:
            rowgen = DBQuery(
                dbconn=aisdatabase, start=start, end=end, **domain.boundary,
                callback=callback, mmsi=316000000, mmsis=[316000000, 316000001]
            ).gen_qry(fcn=sqlfcn.crawl_dynamic_static)
            next(rowgen)

# def test_noaa_data_ingest_postgres_only(tmpdir):
#     testdatacsv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_noaa_20230101.csv")
#     filepaths = [testdatacsv]

#     start_time = datetime(2023, 1, 1)
#     end_time = datetime(2023, 1, 31)

#     with PostgresDBConn(conn_information) as pgdb:
#         decode_msgs(
#             filepaths,
#             dbconn=pgdb,
#             source='NOAA',
#             vacuum=False,
#             verbose=True,
#             skip_checksum=True,
#             raw_insertion=True,
#             timescaledb=True
#         )
#         pgdb.commit()

#         rowgen = DBQuery(
#             dbconn=pgdb,
#             start=start_time,
#             end=end_time,
#             callback=sqlfcn_callbacks.in_timerange_validmmsi
#         ).gen_qry(reaggregate_static=True)

#         tracks = list(TrackGen(rowgen, decimate=False))

#     assert len(tracks) > 0
#     print(f"NOAA PostgreSQL query produced {len(tracks)} tracks")
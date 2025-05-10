import os
from datetime import datetime, timedelta

from shapely.geometry import Polygon

from aisdb import (DBConn, DBQuery, Domain, PostgresDBConn, sqlfcn, sqlfcn_callbacks, )
from aisdb.database.decoder import decode_msgs
from aisdb.tests.create_testing_data import (sample_database_file, sample_gulfstlawrence_bbox, )
from aisdb.track_gen import TrackGen
import urllib.parse

USER = os.environ['pguser']
PASSWORD = urllib.parse.quote_plus(os.environ['pgpass'])
ADDRESS = os.environ['pghost']
PORT = 5432
DBNAME = os.environ['db_name']

conn_information = f"postgresql://{USER}:{PASSWORD}@{ADDRESS}:{PORT}/{DBNAME}"

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

        cur.execute("SELECT COUNT(*) FROM ais_global_dynamic;")
        count = cur.fetchone()[0]
        print(f"Row count: {count}")
        assert count > 0

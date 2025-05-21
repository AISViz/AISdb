import os
from datetime import datetime
import urllib
from aisdb.database.dbconn import PostgresDBConn
from aisdb.database.decoder import decode_msgs

# Assumes these are set in your environment
# pguser, pgpass, pghost

USER = os.environ["pguser"]
PASSWORD = urllib.parse.quote_plus(os.environ["pgpass"])
ADDRESS = '127.0.0.1'
PORT = 5432
DBNAME = os.environ["dbname"]

def test_decode_1day_postgres():
    conn_str = (
        f"postgresql://{USER}:{PASSWORD}@{ADDRESS}:{PORT}/{DBNAME}"
    )

    testing_data_zip = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.zip")
    testing_data_gz = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.gz")
    testing_data_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    testing_data_nm4 = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4")
    testing_data_nmea = os.path.join(os.path.dirname(__file__), "testdata", "test_data_201201.nmea")

    filepaths = [testing_data_nmea, testing_data_nm4, testing_data_csv, testing_data_gz, testing_data_zip]

    with PostgresDBConn(conn_str) as db_conn:
        dt = datetime.now()

        decode_msgs(filepaths=filepaths, dbconn=db_conn, source="TESTING", vacuum=False, timescaledb=True)

        delta = datetime.now() - dt
        print(f"timescaledb total parse and insert time: {delta.total_seconds():.2f}s")
import os
from datetime import datetime

from aisdb.database.dbconn import PostgresDBConn
from aisdb.database.decoder import decode_msgs

def test_decode_1day():
    conn_string = (f"postgresql://{os.environ['pguser']}:{os.environ['pgpass']}@"
                    f"{os.environ['pghost']}:5432/{os.environ['pguser']}")
    db_conn = PostgresDBConn(conn_string)

    base_dir = os.path.join(os.path.dirname(__file__), "testdata")
    filepaths = [
        os.path.join(base_dir, "test_data_201201.nmea"),
        os.path.join(base_dir, "test_data_20211101.nm4"),
        os.path.join(base_dir, "test_data_20210701.csv"),
        os.path.join(base_dir, "test_data_20211101.nm4.gz"),
        os.path.join(base_dir, "test_data_20211101.nm4.zip"),
    ]

    print("\n ---> Using PostgreSQL connection:", conn_string)

    with db_conn:
        dt = datetime.now()
        decode_msgs(filepaths=filepaths, dbconn=db_conn, source="TESTING", vacuum=True, timescaledb=True)
        decode_msgs(filepaths=filepaths, dbconn=db_conn, source="TESTING", vacuum=False, timescaledb=True)
        delta = datetime.now() - dt
        print(f"PostgreSQL total parse and insert time: {delta.total_seconds():.2f}s")

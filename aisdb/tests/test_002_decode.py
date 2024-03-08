import os
from datetime import datetime

from aisdb.database.dbconn import DBConn
from aisdb.database.decoder import decode_msgs


def test_decode_1day(tmpdir):
    dbpath = os.path.join(tmpdir, "test_decode_1day.db")
    testing_data_zip = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.zip")
    testing_data_gz = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4.gz")
    testing_data_csv = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20210701.csv")
    testing_data_nm4 = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4")
    print("\n ---> ", dbpath)

    with DBConn(dbpath) as db_conn:
        filepaths = [testing_data_nm4, testing_data_csv, testing_data_gz, testing_data_zip]
        dt = datetime.now()
        decode_msgs(filepaths=filepaths, dbconn=db_conn, source="TESTING", vacuum=True)
        decode_msgs(filepaths=filepaths, dbconn=db_conn, source="TESTING", vacuum=dbpath + ".vacuum")
        delta = datetime.now() - dt
        print(f"sqlite total parse and insert time: {delta.total_seconds():.2f}s")

import os
from datetime import datetime

from aisdb.database.dbconn import DBConn, PostgresDBConn
from aisdb.database.decoder import decode_msgs


def test_decode_1day(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_decode_1day.db')
    testingdata_nm4 = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4')
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    testingdata_gz = os.path.join(os.path.dirname(__file__), 'testdata',
                                  'test_data_20211101.nm4.gz')
    testingdata_zip = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4.zip')
    with DBConn() as dbconn:
        filepaths = [
            testingdata_nm4, testingdata_csv, testingdata_gz, testingdata_zip
        ]
        dt = datetime.now()
        decode_msgs(filepaths=filepaths,
                    dbconn=dbconn,
                    dbpath=dbpath,
                    source='TESTING',
                    vacuum=True)
        decode_msgs(filepaths=filepaths,
                    dbconn=dbconn,
                    dbpath=dbpath,
                    source='TESTING',
                    vacuum=dbpath + '.vacuum')
        delta = datetime.now() - dt
        print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_decode_1day_postgres(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_decode_1day.db')
    testingdata_nm4 = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4')
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    testingdata_gz = os.path.join(os.path.dirname(__file__), 'testdata',
                                  'test_data_20211101.nm4.gz')
    testingdata_zip = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4.zip')
    with PostgresDBConn(
            hostaddr='fc00::17',
            user='postgres',
            port=5432,
            password=os.environ.get('POSTGRES_PASSWORD', 'devel'),
    ) as dbconn:
        filepaths = [
            testingdata_nm4, testingdata_csv, testingdata_gz, testingdata_zip
        ]
        dt = datetime.now()
        decode_msgs(filepaths=filepaths,
                    dbconn=dbconn,
                    source='TESTING',
                    vacuum=True)
        decode_msgs(filepaths=filepaths,
                    dbconn=dbconn,
                    source='TESTING',
                    vacuum=True)
        delta = datetime.now() - dt
        print(f'total parse and insert time: {delta.total_seconds():.2f}s')

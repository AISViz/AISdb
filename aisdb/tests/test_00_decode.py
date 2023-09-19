import os
from datetime import datetime

from aisdb.database.dbconn import DBConn, PostgresDBConn
from aisdb.database.decoder import decode_msgs
from aisdb.tests.create_testing_data import postgres_test_conn


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
    with DBConn(dbpath) as dbconn:
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
                    vacuum=dbpath + '.vacuum')
        delta = datetime.now() - dt
        print(
            f'sqlite total parse and insert time: {delta.total_seconds():.2f}s'
        )


def test_decode_1day_postgres(tmpdir):
    testingdata_nm4 = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4')
    testingdata_csv = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20210701.csv')
    testingdata_gz = os.path.join(os.path.dirname(__file__), 'testdata',
                                  'test_data_20211101.nm4.gz')
    testingdata_zip = os.path.join(os.path.dirname(__file__), 'testdata',
                                   'test_data_20211101.nm4.zip')
    filepaths = [
        testingdata_csv, testingdata_nm4, testingdata_gz, testingdata_zip
    ]

    with PostgresDBConn(**postgres_test_conn) as dbconn:

        #dbconn.execute('TRUNCATE hashmap')
        #dbconn.commit()

        dt = datetime.now()
        decode_msgs(filepaths=filepaths,
                    dbconn=dbconn,
                    source='TESTING',
                    vacuum=True,
                    verbose=True)
        delta = datetime.now() - dt
        print(
            f'postgres total parse and insert time: {delta.total_seconds():.2f}s'
        )

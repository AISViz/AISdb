import os
from datetime import datetime

from aisdb.database.dbconn import  PostgresDBConn
from aisdb.database.decoder import decode_msgs
from aisdb.tests.create_testing_data import postgres_test_conn

#import dotenv
#dotenv.load_dotenv()


def test_postgres():

    # keyword arguments
    with PostgresDBConn(**postgres_test_conn) as dbconn:
        cur = dbconn.cursor()
        cur.execute('select * from coarsetype_ref;')
        res = cur.fetchall()
        print(res)


# libpq connection string
'''
with PostgresDBConn('postgresql://127.0.0.1:443/postgres?') as dbconn:
    with dbconn.cursor() as cur:
        cur.execute('select * from coarsetype_ref;')
        res = cur.fetchall()
        print(res)
'''


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

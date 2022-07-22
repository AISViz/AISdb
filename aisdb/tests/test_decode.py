import os
from datetime import datetime

from aisdb.database.dbconn import DBConn
from aisdb.database.decoder import decode_msgs


def test_decode_1day(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_decode_1day.db')
    testingdata_nm4 = os.path.join(os.path.dirname(__file__),
                                   'testingdata_20211101.nm4')
    with DBConn(dbpath=dbpath) as db:
        filepaths = [testingdata_nm4]
        dt = datetime.now()
        decode_msgs(filepaths=filepaths,
                    db=db,
                    dbpath=dbpath,
                    source='TESTING',
                    vacuum=True)
        decode_msgs(filepaths=filepaths,
                    db=db,
                    dbpath=dbpath,
                    source='TESTING',
                    vacuum=dbpath + '.vacuum')
        delta = datetime.now() - dt
        print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_decode_csv(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_decode_csv.db')
    testingdata_csv = os.path.join(os.path.dirname(__file__),
                                   'testingdata_20210701.csv')
    with DBConn(dbpath=dbpath) as db:
        filepaths = [testingdata_csv]
        dt = datetime.now()
        decode_msgs(filepaths=filepaths,
                    db=db,
                    dbpath=dbpath,
                    source='TESTING',
                    vacuum=True)
        decode_msgs(filepaths=filepaths,
                    db=db,
                    dbpath=dbpath,
                    source='TESTING',
                    vacuum=dbpath + '.vacuum')
        delta = datetime.now() - dt
        print(f'total parse and insert time: {delta.total_seconds():.2f}s')

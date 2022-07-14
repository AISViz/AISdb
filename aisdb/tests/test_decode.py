import os
from datetime import datetime

from aisdb.database.dbconn import DBConn
from aisdb.database.decoder import decode_msgs

from aisdb.tests.create_testing_data import create_testing_aisdata


def test_sort_1d(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_decode.db')
    with DBConn(dbpath=dbpath) as db:
        create_testing_aisdata(tmpdir)
        filepaths = [os.path.join(tmpdir, 'testingdata.nm4')]
        dt = datetime.now()
        decode_msgs(filepaths=filepaths,
                    db=db,
                    dbpath=dbpath,
                    source='TESTING')
        delta = datetime.now() - dt
        print(f'total parse and insert time: {delta.total_seconds():.2f}s')

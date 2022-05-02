import os
from datetime import datetime

from aisdb import data_dir
from aisdb.database.decoder import decode_msgs

from aisdb.tests.create_testing_data import create_testing_aisdata

testdb = os.path.join(os.path.dirname(data_dir), 'testdb')

if not os.path.isdir(testdb):
    os.mkdir(testdb)


def test_sort_1d():

    create_testing_aisdata()

    db = os.path.join(testdb + 'test.db')

    filepaths = [os.path.join(testdb, 'testingdata.nm4')]
    print(filepaths)
    dt = datetime.now()
    decode_msgs(filepaths, db)
    delta = datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')

    os.remove(db)

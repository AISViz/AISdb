import os
from datetime import datetime

from aisdb.database.decoder import decode_msgs

from aisdb.tests.create_testing_data import create_testing_aisdata


def test_sort_1d(tmpdir):
    print(tmpdir)
    db = os.path.join(tmpdir, 'test_decode.db')

    create_testing_aisdata(tmpdir)

    filepaths = [os.path.join(tmpdir, 'testingdata.nm4')]
    print(filepaths)
    dt = datetime.now()
    decode_msgs(filepaths, db, 'TESTING')
    delta = datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')

    os.remove(db)

import os
from datetime import timedelta, datetime

from aisdb import dbpath, rawdata_dir, tmp_dir
from aisdb.database.decoder import decode_msgs

from aisdb.proc_util import glob_files, getfiledate

from tests.create_testing_data import create_testing_aisdata

testdbs = os.path.join(os.path.dirname(dbpath), 'testdb') + os.path.sep

if not os.path.isdir(testdbs):
    os.mkdir(testdbs)


def test_sort_1d():

    create_testing_aisdata()

    db = testdbs + 'test_12h.db'

    filepaths = glob_files(rawdata_dir, ext='.nm4')
    testset = [
        f for f in filepaths
        if getfiledate(f) - getfiledate(filepaths[0]) <= timedelta(hours=12)
    ]
    dt = datetime.now()
    decode_msgs(testset, db)
    delta = datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')

    os.remove(db)

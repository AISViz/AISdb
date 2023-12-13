import aisdb
import os
from datetime import datetime, timedelta

import numpy as np

from aisdb import track_gen, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb.database import sqlfcn
from aisdb.tests.create_testing_data import sample_database_file
from aisdb.webdata.marinetraffic import vessel_info, VesselInfo

testdir = os.environ.get(
    'AISDBTESTDIR',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
    ),
)
if not os.path.isdir(testdir):
    os.mkdir(testdir)

trafficDBpath = os.path.join(testdir, 'marinetraffic_test.db')


def test_epoch_dt_convert():
    aisdb.proc_util._epoch_2_dt(1600000000)
    aisdb.proc_util._epoch_2_dt([1600000000])


def test_write_csv_rows(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_write_csv.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn(dbpath) as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(verbose=True)
        aisdb.proc_util.write_csv_rows(
            rowgen,
            pathname=os.path.join(
                tmpdir,
                'test_write_csv_rows.csv',
            ),
        )


def test_write_csv_fromdict(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_write_csv.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn(dbpath) as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )

        rowgen = qry.gen_qry(fcn=sqlfcn.crawl_dynamic, verbose=True)
        tracks = track_gen.TrackGen(rowgen, decimate=True)
        aisdb.proc_util.write_csv(tracks,
                                  fpath=os.path.join(tmpdir,
                                                     'test_write_csv.csv'))


def test_write_csv_fromdict_marinetraffic(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_write_csv.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    vinfo_db = VesselInfo(trafficDBpath).trafficDB

    with DBConn(dbpath) as dbconn, vinfo_db as trafficDB:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        qry.check_marinetraffic (trafficDBpath=trafficDBpath,
                                boundary={
                                    'xmin': -45,
                                    'xmax': -25,
                                    'ymin': 30,
                                    'ymax': 50,
                                })

        rowgen = qry.gen_qry(fcn=sqlfcn.crawl_dynamic_static, verbose=True)
        tracks = vessel_info(track_gen.TrackGen(rowgen, decimate=True),
                             trafficDB)
        aisdb.proc_util.write_csv(tracks,
                                  fpath=os.path.join(tmpdir,
                                                     'test_write_csv.csv'))


def test_glob_files():
    aisdb.proc_util.glob_files(os.path.dirname(__file__), '.nm4')


def test_getfiledate():
    aisdb.proc_util.getfiledate(
        os.path.join(os.path.dirname(__file__), 'testdata',
                     'test_data_20211101.nm4'))
    aisdb.proc_util.getfiledate(
        os.path.join(os.path.dirname(__file__), 'testdata',
                     'test_data_20210701.csv'))


def test_binarysearch():
    arr = np.array([1, 2, 3])
    arr_desc = arr[::-1]
    assert aisdb.aisdb.binarysearch_vector(arr, [2])[0] == 1
    assert aisdb.aisdb.binarysearch_vector(arr, [5])[0] == 2
    assert aisdb.aisdb.binarysearch_vector(arr, [-10])[0] == 0
    assert aisdb.aisdb.binarysearch_vector(arr_desc, [10])[0] == 0
    assert aisdb.aisdb.binarysearch_vector(arr_desc, [-5])[0] == 2
    assert aisdb.aisdb.binarysearch_vector(arr_desc, [2])[0] == 1

import aisdb
import os
from datetime import datetime

from aisdb import track_gen, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb.database import sqlfcn
from aisdb.tests.create_testing_data import sample_database_file
from aisdb.webdata.marinetraffic import vessel_info

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)

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
    sample_database_file(dbpath)

    with DBConn(dbpath=dbpath) as db:
        qry = DBQuery(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(dbpath, printqry=True)
        aisdb.proc_util.write_csv_rows(
            rowgen,
            pathname=os.path.join(
                tmpdir,
                'test_write_csv_rows.csv',
            ),
        )


def test_write_csv_fromdict_marinetraffic(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_write_csv.db')
    sample_database_file(dbpath)

    with DBConn(dbpath=dbpath) as db:
        qry = DBQuery(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )

        rowgen = qry.gen_qry(dbpath,
                             fcn=sqlfcn.crawl_dynamic_static,
                             printqry=True)
        tracks = vessel_info(track_gen.TrackGen(rowgen), trafficDBpath)
        aisdb.proc_util.write_csv(tracks,
                                  fpath=os.path.join(tmpdir,
                                                     'test_write_csv.csv'))


def test_glob_files():
    dbs = aisdb.proc_util.glob_files(testdir, '.db')


def test_getfiledate():
    aisdb.proc_util.getfiledate(os.path.join(testdir, 'testingdata.nm4'))

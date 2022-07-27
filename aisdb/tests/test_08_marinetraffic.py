import os
from datetime import datetime

from aisdb import track_gen, decode_msgs, DBQuery, sqlfcn_callbacks
from aisdb.webdata.marinetraffic import vessel_info
from aisdb.tests.create_testing_data import random_polygons_domain
from aisdb import DBConn

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


def test_init_scraper():
    from aisdb.webdata._scraper import _scraper
    _driver = _scraper(testdir)
    _driver.close()


def test_retrieve_marinetraffic_data(tmpdir):
    domain = random_polygons_domain(count=10)

    datapath = os.path.join(os.path.dirname(__file__),
                            'test_data_20211101.nm4')
    dbpath = os.path.join(tmpdir, 'test_retrieve_marinetraffic_data.db')
    with DBConn() as dbconn:
        decode_msgs(filepaths=[datapath],
                    dbconn=dbconn,
                    dbpath=dbpath,
                    source='TESTING')

        qry = DBQuery(dbconn=dbconn,
                      dbpath=dbpath,
                      start=start,
                      end=end,
                      callback=sqlfcn_callbacks.in_timerange_validmmsi)
        qry.check_marinetraffic(dbpath=dbpath,
                                trafficDBpath=trafficDBpath,
                                data_dir=testdir,
                                boundary=domain.boundary,
                                retry_404=False)
        rowgen = qry.gen_qry(printqry=True)
        tracks = track_gen.TrackGen(rowgen)

        try:

            for track in vessel_info(tracks, trafficDBpath):
                assert 'marinetraffic_info' in track.keys()
        except UserWarning:
            pass
        except Exception as err:
            raise err


def test_marinetraffic_metadict():
    #trafficDBpath = '/RAID0/ais/marinetraffic_V2.db'
    with DBConn() as dbconn:
        vessel_info(dbconn, trafficDBpath)

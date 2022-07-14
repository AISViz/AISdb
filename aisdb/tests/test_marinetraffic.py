import os
from datetime import datetime

from aisdb import track_gen, decode_msgs, DBQuery, sqlfcn_callbacks
from aisdb.webdata.marinetraffic import _metadict, vessel_info
from aisdb.tests.create_testing_data import random_polygons_domain
from aisdb import DBConn

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)

trafficDBpath = os.path.join(os.path.dirname(__file__),
                             'marinetraffic_test.db')


def test_retrieve_marinetraffic_data(tmpdir):
    webdriverpath = os.path.join(os.path.dirname(__file__), 'webdriver')
    domain = random_polygons_domain(count=10)

    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')

    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    with DBConn(dbpath=dbpath) as db:
        decode_msgs(filepaths=[datapath],
                    db=db,
                    dbpath=dbpath,
                    source='TESTING')

        qry = DBQuery(db=db,
                      dbpath=dbpath,
                      start=start,
                      end=end,
                      callback=sqlfcn_callbacks.in_timerange_validmmsi)
        qry.check_marinetraffic(trafficDBpath,
                                data_dir=webdriverpath,
                                boundary=domain.boundary,
                                retry_404=False)
        rowgen = qry.gen_qry(dbpath, printqry=True)
        tracks = track_gen.TrackGen(rowgen)

        for track in vessel_info(tracks, trafficDBpath):
            assert 'marinetraffic_info' in track.keys()


def test_marinetraffic_metadict(tmpdir):
    #trafficDBpath = '/RAID0/ais/marinetraffic_V2.db'
    _metadict(trafficDBpath)

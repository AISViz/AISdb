import os
from datetime import datetime

from shapely.geometry import Polygon

from aisdb import track_gen, decode_msgs, DBQuery, sqlfcn_callbacks, Domain
from aisdb.webdata.marinetraffic import vessel_info, _vessel_info_dict, VesselInfo
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox
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
    _driver = _scraper()
    _driver.close()
    _driver.quit()


def test_retrieve_marinetraffic_data(tmpdir):
    #domain = random_polygons_domain(count=10)
    coords = sample_gulfstlawrence_bbox()
    poly = Polygon(zip(*coords))
    domain = Domain(name='test_marinetraffic',
                    zones=[{
                        'name': 'gulfstlawrence',
                        'geometry': poly
                    }])

    datapath = os.path.join(os.path.dirname(__file__), 'testdata',
                            'test_data_20211101.nm4')
    dbpath = os.path.join(tmpdir, 'test_retrieve_marinetraffic_data.db')

    vinfoDB = VesselInfo(trafficDBpath).trafficDB

    with DBConn(dbpath) as dbconn, vinfoDB as trafficDB:
        decode_msgs(filepaths=[datapath], dbconn=dbconn, source='TESTING')

        qry = DBQuery(dbconn=dbconn,
                      start=start,
                      end=end,
                      callback=sqlfcn_callbacks.in_timerange_validmmsi)
        qry.check_marinetraffic(trafficDBpath=trafficDBpath,
                                boundary=domain.boundary,
                                retry_404=False)
        rowgen = qry.gen_qry(verbose=True)
        trackgen = track_gen.TrackGen(rowgen, decimate=True)
        tracks = [next(trackgen), next(trackgen)]

        try:

            for track in vessel_info(tracks, trafficDB):
                assert 'marinetraffic_info' in track.keys()
        except UserWarning:
            pass
        except Exception as err:
            raise err


def test_marinetraffic_metadict():
    trafficDB = VesselInfo(trafficDBpath).trafficDB
    meta = _vessel_info_dict(trafficDB)
    assert meta

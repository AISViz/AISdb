from datetime import datetime

from shapely.geometry import Polygon

from aisdb import DBConn
from aisdb import track_gen, decode_msgs, DBQuery, sqlfcn_callbacks, Domain
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox
from aisdb.webdata._scraper import *
from aisdb.webdata.marinetraffic import vessel_info, _vessel_info_dict, VesselInfo

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)

testdir = os.environ.get("AISDBTESTDIR",
                         os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "testdata", ), )
if not os.path.isdir(testdir):
    os.mkdir(testdir)

trafficDBpath = os.path.join(testdir, "marinetraffic_test.db")


def test_retrieve_marinetraffic_data(tmpdir=testdir):
    # domain = random_polygons_domain(count=10)
    coords = sample_gulfstlawrence_bbox()
    poly = Polygon(zip(*coords))
    domain = Domain(name="test_marinetraffic", zones=[{"name": "gulfstlawrence", "geometry": poly}])

    datapath = os.path.join(os.path.dirname(__file__), "testdata", "test_data_20211101.nm4")
    dbpath = os.path.join(tmpdir, "test_retrieve_marinetraffic_data.db")
    print("dbpath: {0}".format(dbpath))
    print("datapath: {0}".format(datapath))
    print("trafficDBpath: {0}".format(trafficDBpath))
    vinfoDB = VesselInfo(trafficDBpath).trafficDB

    with DBConn(dbpath) as dbconn:
        decode_msgs(filepaths=[datapath], dbconn=dbconn, source="TESTING", verbose=True)

    with DBConn(dbpath) as dbconn, vinfoDB as trafficDB:

        qry = DBQuery(dbconn=dbconn, start=start, end=end, callback=sqlfcn_callbacks.in_timerange_validmmsi)
        qry.check_marinetraffic(trafficDBpath=trafficDBpath, boundary=domain.boundary, retry_404=False)
        rowgen = qry.gen_qry(verbose=True)
        trackgen = track_gen.TrackGen(rowgen, decimate=True)
        tracks = []
        tracks.append(next(trackgen))
        tracks.append(next(trackgen))

        try:

            for track in vessel_info(tracks, trafficDB):
                assert "marinetraffic_info" in track.keys()
        except UserWarning:
            pass
        except Exception as err:
            raise err


def test_init_scraper():
    from aisdb.webdata._scraper import _scraper
    _driver = _scraper()
    _driver.close()
    _driver.quit()


def test_marinetraffic_metadict():
    ves_info = VesselInfo(trafficDBpath)
    trafficDB = ves_info.trafficDB
    meta = _vessel_info_dict(trafficDB)
    assert meta


def test_vessel_finder():
    MMSI = 240927000
    dict_ = search_metadata_vesselfinder(MMSI)
    dict_2 = search_metadata_marinetraffic(MMSI)

    assert dict_
    assert dict_2

'''
from multiprocessing import set_start_method
set_start_method('forkserver')
from multiprocessing import Pool, Queue
'''
import os
from datetime import datetime, timedelta
from functools import partial

from shapely.geometry import Polygon
import numpy as np

from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.dbqry import DBQuery, DBConn
from aisdb.gis import Domain
from aisdb.network_graph import graph, _pipeline
from aisdb.track_gen import (
    TrackGen,
    deserialize_tracks,
    encode_greatcircledistance,
    fence_tracks,
    serialize_tracks,
)
from aisdb.tests.create_testing_data import (
    sample_database_file,
    sample_gulfstlawrence_bbox,
)
from aisdb.webdata.marinetraffic import vessel_info
from aisdb.wsa import wetted_surface_area

trafficDBpath = os.environ.get(
    'AISDBMARINETRAFFIC',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
        'marinetraffic_test.db',
    ))
data_dir = os.environ.get(
    'AISDBDATADIR',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
    ),
)

lon, lat = sample_gulfstlawrence_bbox()
z1 = Polygon(zip(lon, lat))
z2 = Polygon(zip(lon + 90, lat))
z3 = Polygon(zip(lon, lat - 45))
domain = Domain('gulf domain',
                zones=[
                    {
                        'name': 'z1',
                        'geometry': z1
                    },
                    {
                        'name': 'z2',
                        'geometry': z2
                    },
                    {
                        'name': 'z3',
                        'geometry': z3
                    },
                ])


def test_geofencing(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_geofencing.db')
    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn() as aisdatabase:
        # query configs
        rowgen = DBQuery(
            dbconn=aisdatabase,
            dbpath=testdbpath,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_bbox,
        )

        # processing configs
        distsplit = partial(
            encode_greatcircledistance,
            distance_threshold=250000,
            speed_threshold=45,
            minscore=5e-07,
        )
        geofenced = partial(fence_tracks, domain=domain)

        # query db for points in domain bounding box
        _test = next(geofenced(distsplit(TrackGen(rowgen.gen_qry()))))


def test_graph_CSV_single(tmpdir):

    testdbpath = os.path.join(
        tmpdir, 'test_network_graph_CSV_single_marinetraffic.db')
    outpath = os.path.join(tmpdir, 'output.csv')

    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with DBConn() as aisdatabase:
        qry = DBQuery(
            dbconn=aisdatabase,
            dbpath=testdbpath,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_bbox,
            fcn=sqlfcn.crawl_dynamic_static,
        )

        graph(
            qry,
            data_dir=data_dir,
            domain=domain,
            dbpath=testdbpath,
            trafficDBpath=trafficDBpath,
            processes=0,
            outputfile=outpath,
            maxdelta=timedelta(weeks=1),
        )
        assert os.path.isfile(outpath)
    with open(outpath, 'r') as out:
        print(out.read())


def test_graph_CSV_parallel(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_network_graph_CSV_parallel.db')
    outpath = os.path.join(tmpdir, 'output.csv')

    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    with DBConn() as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            dbpath=testdbpath,
            start=start,
            end=end,
            xmin=-180,
            xmax=180,
            ymin=-90,
            ymax=90,
            callback=sqlfcn_callbacks.in_bbox,
            fcn=sqlfcn.crawl_dynamic_static,
        )
        _ = graph(
            qry,
            domain=domain,
            data_dir=data_dir,
            dbpath=testdbpath,
            trafficDBpath=trafficDBpath,
            processes=6,
            outputfile=outpath,
            maxdelta=timedelta(weeks=1),
        )
        assert os.path.isfile(outpath)
    with open(outpath, 'r') as out:
        print(out.read())


def test_serialize_deserialize_tracks():
    track = dict(
        lon=(np.random.random(10) * 90) - 90,
        lat=(np.random.random(10) * 90) + 0,
        time=np.array(range(10)),
        dynamic=set(['time', 'lon', 'lat']),
        static=set(['mmsi', 'ship_type']),
        mmsi=316000000,
        ship_type='test',
    )
    for track in deserialize_tracks(serialize_tracks([track])):
        assert isinstance(track, dict)


def test_graph_pipeline_timing(tmpdir):
    count = 10000
    track = dict(
        lon=(np.random.random(count) * 90) - 90,
        lat=(np.random.random(count) * 90) + 0,
        time=np.array(range(count)),
        dynamic=set(['time', 'lon', 'lat']),
        static=set(['mmsi', 'ship_type']),
        mmsi=316000000,
        ship_type='test',
    )
    track = next(wetted_surface_area(vessel_info([track], trafficDBpath)))
    _pipeline(track,
              domain=domain,
              tmp_dir=tmpdir,
              maxdelta=timedelta(weeks=1),
              distance_threshold=250000,
              speed_threshold=50,
              minscore=0)


'''
    import tempfile
    import cProfile

    data_dir = '/RAID0/ais/'
    trafficDBpath = './testdata/marinetraffic_test.db'

    with tempfile.TemporaryDirectory() as tmpdir:
        cProfile.run("test_graph_CSV_single(tmpdir)", sort="tottime")
        cProfile.run("test_graph_CSV_parallel(tmpdir)", sort="tottime")
'''

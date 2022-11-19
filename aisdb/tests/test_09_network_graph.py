'''
from multiprocessing import set_start_method
set_start_method('forkserver')
from multiprocessing import Pool, Queue
'''
import os
from datetime import datetime, timedelta
from functools import partial, reduce
from multiprocessing import Queue

from shapely.geometry import Polygon
import numpy as np

from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.dbqry import DBQuery, DBConn
from aisdb.gis import Domain
from aisdb.network_graph import (
    graph,
    _processing_pipeline,
)
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


def test_fence_tracks(tmpdir):
    tracks = [
        {
            'mmsi': -1,
            'lon': np.array([z1.centroid.x, z2.centroid.x, z3.centroid.x]),
            'lat': np.array([z1.centroid.y, z2.centroid.y, z3.centroid.y]),
            'time': np.array(list(range(0, 3 * 1000, 1000))),
            'dynamic': set(['lon', 'lat', 'time']),
            'static': {'mmsi'},
        },
    ]
    tracks_fenced = fence_tracks(tracks, domain)
    test = next(tracks_fenced)['in_zone']
    assert test[0] == 'z1'
    assert test[1] == 'z2'
    assert test[2] == 'z3'


def test_fence_tracks_realdata(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_fence_tracks_realdata.db')
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
        _test = np.array(
            list(
                geofenced(distsplit(TrackGen(rowgen.gen_qry(),
                                             decimate=0.01)))))
        zoneset = reduce(set.union, (set(track['in_zone']) for track in _test))
        assert zoneset != {'Z0'}
        zonemask = [set(track['in_zone']) != {'Z0'} for track in _test]


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


def setup_network_graph(tmpdir, processes):
    testdbpath = os.path.join(tmpdir, 'test_network_graph_CSV.db')
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
            callback=sqlfcn_callbacks.in_bbox,
            fcn=sqlfcn.crawl_dynamic_static,
            **domain.boundary,
        )

        graph(
            qry,
            data_dir=data_dir,
            domain=domain,
            dbpath=testdbpath,
            trafficDBpath=trafficDBpath,
            processes=processes,
            outputfile=outpath,
            maxdelta=timedelta(weeks=1),
            decimate=0.01,
        )
        assert os.path.isfile(outpath)
        with open(outpath, 'r') as out:
            #print(out.read())
            assert (out.read())
    os.remove(outpath)


def test_network_graph_CSV_single(tmpdir):
    setup_network_graph(tmpdir, 1)


def test_network_graph_CSV_parallel(tmpdir):
    setup_network_graph(tmpdir, 2)


def test_graph_pipeline_timing(tmpdir):
    count = 1000
    track = dict(
        lon=(np.random.random(count) * .25) - 45,
        lat=(np.random.random(count) * .25) + 45,
        time=np.array(range(count, count * 30, 30)),
        dynamic=set(['time', 'lon', 'lat']),
        static=set(['mmsi', 'ship_type']),
        mmsi=316000000,
        ship_type='test',
    )
    serialized = serialize_tracks([track() for _ in range(10)])

    _processing_pipeline(
        serialized,
        data_dir=data_dir,
        domain=domain,
        tmp_dir=tmpdir,
        maxdelta=timedelta(weeks=1),
        distance_threshold=250000,
        speed_threshold=50,
        minscore=0,
        trafficDBpath=trafficDBpath,
        #shoredist_raster=os.path.join(data_dir, 'distance-from-shore.tif'),
        portdist_raster=os.path.join(data_dir,
                                     'distance-from-port-v20201104.tiff'),
        interp_delta=timedelta(seconds=1),
    )
    #q.close()


'''
    import tempfile
    import cProfile

    data_dir = '/RAID0/ais/'
    trafficDBpath = './testdata/marinetraffic_test.db'

    with tempfile.TemporaryDirectory() as tmpdir:
        cProfile.run("test_graph_pipeline_timing(tmpdir)", sort="tottime")
        #cProfile.run("test_network_graph_CSV(tmpdir)", sort="tottime")
'''

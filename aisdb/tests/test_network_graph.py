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

from aisdb.proc_util import getfiledate
from aisdb.database.dbqry import DBQuery, DBConn
from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.decoder import decode_msgs
from aisdb.gis import Domain
from aisdb.track_gen import (
    fence_tracks,
    encode_greatcircledistance,
    TrackGen,
)
from aisdb.network_graph import graph, _pipeline
from aisdb.tests.create_testing_data import (
    sample_gulfstlawrence_bbox, )

testingdata_nm4 = os.path.join(os.path.dirname(__file__),
                               'testingdata_20211101.nm4')
testingdata_csv = os.path.join(os.path.dirname(__file__),
                               'testingdata_20210701.csv')

trafficDBpath = os.environ.get(
    'AISDBMARINETRAFFIC',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
        'marinetraffic_test.db',
    ))

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
    start = datetime(*getfiledate(testingdata_nm4).timetuple()[0:6])
    end = start + timedelta(weeks=4)
    with DBConn(dbpath=testdbpath) as aisdatabase:
        # query configs
        decode_msgs([testingdata_csv, testingdata_nm4],
                    aisdatabase,
                    testdbpath,
                    source='TESTING')

        rowgen = DBQuery(
            db=aisdatabase,
            start=start,
            end=end,
            xmin=-180,  #domain.minX,
            xmax=180,  #domain.maxX,
            ymin=-90,  #domain.minY,
            ymax=90,  #domain.maxY,
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
        _test = next(
            geofenced(distsplit(TrackGen(rowgen.gen_qry(dbpath=testdbpath)))))


def test_graph_CSV_marinetraffic(tmpdir):
    testdbpath = os.path.join(tmpdir,
                              'test_network_graph_CSV_marinetraffic.db')
    outpath = os.path.join(tmpdir, 'output.csv')

    start = datetime(*getfiledate(testingdata_nm4).timetuple()[0:6])
    end = start + timedelta(weeks=4)

    with DBConn(dbpath=testdbpath) as aisdatabase:
        decode_msgs([testingdata_csv, testingdata_nm4],
                    aisdatabase,
                    testdbpath,
                    source='TESTING')
        qry = DBQuery(
            db=aisdatabase,
            start=start,
            end=end,
            xmin=-180,  #domain.minX,
            xmax=180,  #domain.maxX,
            ymin=-90,  #domain.minY,
            ymax=90,  #domain.maxY,
            callback=sqlfcn_callbacks.in_bbox,
            fcn=sqlfcn.crawl_dynamic_static,
        )
        test = next(qry.gen_qry(dbpath=testdbpath))

        graph(
            qry,
            domain=domain,
            dbpath=testdbpath,
            trafficDBpath=trafficDBpath,
            processes=0,
            outputfile=outpath,
            maxdelta=timedelta(weeks=1),
        )
    assert os.path.isfile(outpath)


def test_graph_pipeline_timing(tmpdir):
    lon1M = (np.random.random(1000000) * 90) - 90
    lat1M = (np.random.random(1000000) * 90) + 0
    track1M = [
        dict(
            lon=lon1M,
            lat=lat1M,
            time=range(len(lon1M)),
            dynamic=set(['time', 'lon', 'lat']),
            static=set(['mmsi']),
            mmsi=316000000,
        )
    ]
    _pipeline(track1M[0],
              domain=domain,
              tmp_dir=tmpdir,
              maxdelta=timedelta(weeks=1),
              distance_threshold=250000,
              speed_threshold=50,
              minscore=0)


def test_graph_CSV_parallel_marinetraffic(tmpdir):
    testdbpath = os.path.join(
        tmpdir, 'test_network_graph_CSV_parallel_marinetraffic.db')
    outpath = os.path.join(tmpdir, 'output.csv')

    start = datetime(
        *getfiledate(testingdata_nm4).timetuple()[0:6]) - timedelta(weeks=1)
    end = start + timedelta(weeks=4)
    with DBConn(dbpath=testdbpath) as aisdatabase:
        decode_msgs([testingdata_csv, testingdata_nm4],
                    aisdatabase,
                    testdbpath,
                    source='TESTING')
        qry = DBQuery(
            db=aisdatabase,
            start=start,
            end=end,
            xmin=-180,  #domain.minX,
            xmax=180,  #domain.maxX,
            ymin=-90,  #domain.minY,
            ymax=90,  #domain.maxY,
            callback=sqlfcn_callbacks.in_bbox,
            fcn=sqlfcn.crawl_dynamic_static,
        )
        test = next(qry.gen_qry(dbpath=testdbpath))

        graph(
            qry,
            domain=domain,
            dbpath=testdbpath,
            trafficDBpath=trafficDBpath,
            processes=4,
            outputfile=outpath,
            maxdelta=timedelta(weeks=1),
        )
    assert os.path.isfile(outpath)

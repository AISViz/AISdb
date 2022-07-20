'''
from multiprocessing import set_start_method
set_start_method('forkserver')
from multiprocessing import Pool, Queue
'''
import os
from datetime import datetime, timedelta
from functools import partial

from shapely.geometry import Polygon

from aisdb.database.dbqry import DBQuery, DBConn
from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.gis import Domain
from aisdb.track_gen import (
    fence_tracks,
    encode_greatcircledistance,
    TrackGen,
)
from aisdb.network_graph import graph
from aisdb.webdata.merge_data import (
    merge_tracks_bathymetry,
    merge_tracks_portdist,
    merge_tracks_shoredist,
    # merge_tracks_hullgeom,
)
from aisdb.tests.create_testing_data import (
    sample_gulfstlawrence_bbox,
    sample_database_file,
)

trafficDBpath = os.environ.get(
    'AISDBMARINETRAFFIC',
    os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'tests',
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
    testdbpath = os.path.join(tmpdir, 'test_network_graph.db')
    #aisdatabase = DBConn(dbpath=testdbpath)

    months = sample_database_file(testdbpath)

    year, month = int(months[0][0:4]), int(months[0][4:])
    start = datetime(year, month, 1)
    end = datetime(year, month, 1) + timedelta(weeks=4)
    with DBConn(dbpath=testdbpath) as aisdatabase:
        # query configs

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

        #sample_dynamictable_insertdata(db=aisdatabase, dbpath=testdbpath)
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
    testdbpath = os.path.join(tmpdir, 'test_network_graph.db')
    outpath = os.path.join(tmpdir, 'output.csv')

    #aisdatabase = DBConn(dbpath=testdbpath)

    months = sample_database_file(testdbpath)

    year, month = int(months[0][0:4]), int(months[0][4:])
    start = datetime(year, month, 1)
    end = datetime(year, month, 1) + timedelta(weeks=4)
    with DBConn(dbpath=testdbpath) as aisdatabase:
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


def test_graph_CSV_parallel_marinetraffic(tmpdir):
    testdbpath = os.path.join(tmpdir, 'test_network_graph.db')
    outpath = os.path.join(tmpdir, 'output.csv')

    #aisdatabase = DBConn(dbpath=testdbpath)

    months = sample_database_file(testdbpath)

    year, month = int(months[0][0:4]), int(months[0][4:])
    start = datetime(year, month, 1)
    end = datetime(year, month, 1) + timedelta(weeks=4)
    with DBConn(dbpath=testdbpath) as aisdatabase:
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

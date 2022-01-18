from datetime import datetime, timedelta
from functools import partial
# import cProfile

from aisdb.database.dbqry import DBQuery
from aisdb.database.sqlfcn_callbacks import in_bbox_time_validmmsi
from aisdb.gis import Domain
from aisdb.track_gen import (
    fence_tracks,
    segment_tracks_encode_greatcircledistance,
    TrackGen,
)
from aisdb.network_graph import serialize_network_edge
from tests.create_testing_data import zonegeoms_or_randompoly


def test_network_graph_pipeline():
    # query configs
    start = datetime(2021, 11, 1)
    end = datetime(2021, 12, 1)
    zonegeoms = zonegeoms_or_randompoly(randomize=True, count=10)
    domain = Domain(name='test', geoms=zonegeoms, cache=False)
    args = DBQuery(
        start=start,
        end=end,
        xmin=domain.minX,
        xmax=domain.maxX,
        ymin=domain.minY,
        ymax=domain.maxY,
        callback=in_bbox_time_validmmsi,
    )

    # processing configs
    distsplit = partial(
        segment_tracks_encode_greatcircledistance,
        maxdistance=250000,
        cuttime=timedelta(weeks=1),
        cutknots=45,
        minscore=5e-07,
    )
    geofenced = partial(fence_tracks, domain=domain)
    serialized = partial(serialize_network_edge, domain=domain)

    # query db for points in domain bounding box
    rowgen = args.gen_qry()
    try:
        _test = next(TrackGen(args.gen_qry()))
        _test2 = next(geofenced(distsplit(TrackGen(rowgen))))
        _test3 = next(serialized(geofenced(distsplit(TrackGen(rowgen)))))
    except ValueError as err:
        print('suppressed error due to DBQuery returning empty rows:'
              f'\t{err.with_traceback(None)}')
    except Exception as err:
        raise err


def test_network_graph_pipeline_merged():
    # rowgen = picklegen(fpath)
    # pipeline = serialize(
    #    merge_tracks_bathymetry(
    #        merge_tracks_shoredist(
    #            merge_tracks_hullgeom(geofenced(distsplit(
    #                TrackGen(rowgen)))))))
    pass


'''
import os
os.environ["OMP_NUM_THREADS"] = '1'
os.environ["OPENBLAS_NUM_THREADS"] = '1'
os.environ["MKL_NUM_THREADS"] = '1'
os.environ["VECLIB_MAXIMUM_THREADS"] = '1'
os.environ["NUMEXPR_NUM_THREADS"] = '1'
os.system("taskset -c 0-11 -p %d" % os.getpid())
from multiprocessing import set_start_method
set_start_method('forkserver')
from multiprocessing import Pool, Queue



# from aisdb.webdata.merge_data import (
# merge_tracks_bathymetry,
# merge_tracks_hullgeom,
# merge_tracks_shoredist,
#        )
'''

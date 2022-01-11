from datetime import datetime, timedelta
from functools import partial
# import cProfile

from aisdb.database.qrygen import DBQuery
from aisdb.database.lambdas import in_bbox
from aisdb.gis import Domain
from aisdb.track_gen import (
    fence_tracks,
    segment_tracks_encode_greatcircledistance,
    trackgen,
)
from aisdb.network_graph import serialize_network_edge
from tests.create_testing_data import zonegeoms_or_randompoly

start = datetime(2021, 11, 1)
end = datetime(2021, 12, 1)


def test_network_graph():
    zonegeoms = zonegeoms_or_randompoly(randomize=True, count=10)
    domain = Domain(name='test', geoms=zonegeoms, cache=False)

    # query db for points in domain bounding box
    args = DBQuery(
        start=start,
        end=end,
        xmin=domain.minX,
        xmax=domain.maxX,
        ymin=domain.minY,
        ymax=domain.maxY,
        callback=in_bbox,
    )
    rows = args.run_qry()
    if len(rows) == 0:
        print('no rows found in bbox, exiting...')
        return

    distsplit = partial(segment_tracks_encode_greatcircledistance,
                        maxdistance=250000,
                        cuttime=timedelta(weeks=1),
                        cutknots=45,
                        minscore=5e-07)
    geofenced = partial(fence_tracks, domain=domain)
    #serialize = partial(serialize_network_edge, domain=domain)
    gen = trackgen(rows)
    next(gen)
    #pipeline = serialize(geofenced(distsplit(gen)))
    #next(pipeline)
    next(geofenced(distsplit(gen)))

    # cProfile.run('test = gen.__anext__().send(None)', sort='tottime')
    # cProfile.run('test = next(gen)', sort='tottime')
    #
    # rowgen = picklegen(fpath)
    # pipeline = serialize(
    #    merge_tracks_bathymetry(
    #        merge_tracks_shoredist(
    #            merge_tracks_hullgeom(geofenced(distsplit(
    #                trackgen(rowgen)))))))
    # cProfile.run('test2 = next(pipeline)', sort='tottime')


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

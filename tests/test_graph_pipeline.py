#import asyncio 
#import concurrent.futures

import pickle
import cProfile
from datetime import datetime, timedelta
from functools import partial
from multiprocessing import Pool, Queue

from aisdb import zones_dir, tmp_dir, output_dir
from aisdb.proc_util import *
from aisdb.gis import Domain, ZoneGeomFromTxt
from aisdb.network_graph import serialize_network_edge
from aisdb.track_gen import trackgen, segment_tracks_timesplits, segment_tracks_dbscan, fence_tracks, concat_tracks
from aisdb.merge_data import merge_tracks_hullgeom, merge_tracks_shoredist, merge_tracks_bathymetry

'''
https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
'''


# load domain geometry
shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)
start = datetime(2020,6,1)
end = datetime(2021,10,1)

# configure track geometry pipeline
timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
geofenced = partial(fence_tracks,               domain=domain)
serialize = partial(serialize_network_edge,     domain=domain)


def main():

    '''
    fpath = os.path.join(output_dir, 'rowgen_year_test2.pickle')

    #loop = asyncio.get_event_loop()
    loop = asyncio.get_running_loop()

    #result = await loop.run_in_executor(None, blocking_io, cpu_bound(deserialize_generator(fpath)))
    #async for res in result:
    #    print(res)

    #with concurrent.futures.ProcessPoolExecutor(max_workers=8) as cpus, concurrent.futures.ThreadPoolExecutor(max_workers=8) as disks:
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        return await loop.run_in_executor(pool, blocking_io, deserialize_generator(fpath) )
    '''
        
    
    fpaths = sorted([os.path.join(tmp_dir, 'db_qry', f) for f in os.listdir(os.path.join(tmp_dir, 'db_qry')) if f[:2] == '__'])

    blocking = map(blocking_io, fpaths)


    #blocking = merge_tracks_bathymetry(merge_tracks_shoredist(merge_tracks_hullgeom(trackgen(deserialize(fpaths)))))
    #cProfile.run('next(blocking)', sort='tottime')
    '''
    fcn = partial(cpu_bound)
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as pool:
        #print(type(pool), type(cpu_bound), type(fpaths))
        result = loop.run_in_executor(pool, cpu_bound, fpaths )
        print(result)
    '''

    #with Pool(processes=8) as p:
    #    results = p.imap_unordered(cpu_bound, fpaths)
    #    p.close()
    #    p.join()
    #fcn = lambda fpath: cpu_bound([fpath])

    #for track in blocking:
        #_ = cpu_bound(track)
    #print(next(blocking))

    for x in cpu_bound((next(j for j in i) for i in blocking)):
        list(x)

main()


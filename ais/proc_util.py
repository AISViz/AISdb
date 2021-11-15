import os
import zipfile
from multiprocessing import Pool
from functools import partial
from datetime import datetime, timedelta
import pickle

import numpy as np

from network_graph import serialize_network_edge
from merge_data import merge_layers
from track_gen import trackgen, segment_tracks_timesplits, segment_tracks_dbscan, fence_tracks, concat_tracks

def _fast_unzip(zipf, dirname='.'):
    ''' parallel process worker for fast_unzip() '''
    exists = set(sorted(os.listdir(dirname)))
    with zipfile.ZipFile(zipf, 'r') as zip_ref:
        contents = set(zip_ref.namelist())
        members = list(contents - exists)
        zip_ref.extractall(path=dirname, members=members)


def fast_unzip(zipfilenames, dirname='.', processes=12):
    ''' unzip many files in parallel 
        any existing unzipped files in the target directory will be skipped
    '''
    fcn = partial(_fast_unzip, dirname=dirname)
    with Pool(processes) as p:
        p.imap_unordered(fcn, zipfilenames)
        p.close()
        p.join()


def binarysearch(arr, search):
    ''' fast indexing of ordered arrays '''
    low, high = 0, len(arr)-1
    while (low <= high):
        mid = int((low + high) / 2)
        if arr[mid] == search or mid == 0 or mid == len(arr)-1:
            return mid
        elif (arr[mid] >= search):
            high = mid -1 
        else:
            low = mid +1
    return mid


def writecsv(rows, pathname='/data/smith6/ais/scripts/output.csv', mode='a'):
    with open(pathname, mode) as f: 
        f.write('\n'.join(map(lambda r: ','.join(map(lambda r: r.replace(',','').replace('#',''), map(str.rstrip, map(str, r)))), rows))+'\n')



def deserialize_generator(fpath):
    with open(fpath, 'rb') as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError as e:
                break

def movepickle(fpath):
    with open(fpath, 'rb') as f:
        while True:
            try:
                rows = pickle.load(f)
                with open(os.path.join(tmp_dir, '__'+str(rows[0][0])), 'wb' ) as f2:
                    pickle.dump(rows, f2)
            except EOFError as e:
                break

def deserialize(fpaths):
    for fpath in fpaths:
        assert isinstance(fpath, str), f'fpath = {list(fpath)}'
        print('processing ', fpath)
        with open(fpath, 'rb') as f:
            yield pickle.load(f)



def blocking_io(tracks, domain):
    serialize = partial(serialize_network_edge,     domain=domain)
    for x in serialize(merge_layers(
        #trackgen(deserialize([fpath]))
        tracks
        )):
        yield x

def cpu_bound(fpath, domain):
    #return serialize(geofenced(distsplit(timesplit([track]))))
    #for fpath in fpaths:
    timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
    distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
    geofenced = partial(fence_tracks,               domain=domain)
    for track in geofenced(concat_tracks(distsplit(timesplit(
        #merge_tracks_bathymetry(merge_tracks_shoredist(merge_tracks_hullgeom(trackgen(deserialize(
            trackgen(deserialize([fpath]))
            #tracks
        #    )))))
        )))):
        yield track



### testing
if False:
    from multiprocessing import Queue, Pool
    import time


    def queue_generator(q):
        yield q


    def queue_iterator(q):
        while q.empty():
            time.sleep(1)
            print('sleeping...')
        while not q.empty():
            yield q.get(timeout=10)


    def wrapper(q,):
        callback(merge_layers(queue_iterator(q)))

                
    def merge_layers_parallel(rowgen, callback, processes=8):
        '''
        rowgen yields a set of rows for each MMSI, sorted by time

        example callback:

            ```
            from functools import partial

            from ais.track_gen import segment_tracks_timesplits
            from ais.clustering import segment_tracks_dbscan
            from ais.network_graph import concat_tracks_no_movement

            timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
            distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
            concat    = partial(concat_tracks_no_movement,  domain=domain)

            callback = lambda rowgen: concat(distsplit(timesplit(merged)))
            ```
        '''
        assert False
        q = Queue()

        with Pool(processes=processes) as p:
            #p.imap_unordered(callback, queue_generator(q), chunksize=1)
            p.map(wrapper, queue_generator(q), chunksize=1)
            p.close()
            for rowset in rowgen:
                while q.size() > processes:
                    time.sleep(0.1) 
                q.put(rowset, block=True, timeout=10)

            p.join()


def graph(fpaths, domain, parallel=0):
    ''' perform geofencing on vessel trajectories, then concatenate aggregated 
        transit statistics between nodes (zones) to create network edges from 
        vessel trajectories

        this function will call geofence() for each trajectory in parallel, 
        outputting serialized results to the tmp_dir directory. after 
        deserialization, the temporary files are removed, and output will be 
        written to 'output.csv' inside the data_dir directory

        args:
            tracks: dictionary generator 
                see track_gen.py for examples
            domain: ais.gis.Domain() class object
                collection of zones defined as polygons, these will
                be used as nodes in the network graph
            parallel: integer
                number of processes to compute geofencing in parallel.
                if set to 0 or False, no parallelization will be used
                
        returns: None
    '''
    
    if not parallel: 
        for fpath in fpaths:
            #geofence(track, domain=domain)
            print(fpath)
            for track in blocking_io(cpu_bound(fpath, domain), domain):
                print(track)
    else:
        with Pool(processes=parallel) as p:
            #fcn = partial(geofence, domain=domain)
            #p.map(fcn, (list(m) for m in tracks), chunksize=1)  # better tracebacks for debug
            fcn = partial(cpu_bound, domain=domain)
            results = p.imap_unordered(fcn, tracks, chunksize=1)
            print(results[0])
            print(results)
            p.close()
            p.join()


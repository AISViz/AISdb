import os
import zipfile
from multiprocessing import Pool
from functools import partial
from datetime import datetime, timedelta
import pickle

import numpy as np

from common import output_dir
from network_graph import serialize_network_edge
from track_gen import trackgen, segment_tracks_timesplits, fence_tracks, max_tracklength, segment_tracks_encode_greatcircledistance

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


def binarysearch(arr, search, descending=False):
    ''' fast indexing of ordered arrays '''
    low, high = 0, arr.size-1
    if descending: 
        arr = arr[::-1]
    while (low <= high):
        mid = (low + high) // 2
        if search >= arr[mid-1] and search <= arr[mid+1]: 
            break
        elif (arr[mid] > search):
            high = mid -1 
        else:
            low = mid +1
    if descending: 
        return arr.size - mid - 1
    else:
        return mid


def writecsv(rows, pathname='/data/smith6/ais/scripts/output.csv', mode='a'):
    with open(pathname, mode) as f: 
        f.write('\n'.join(map(lambda r: ','.join(map(lambda r: r.replace(',','').replace('#',''), map(str.rstrip, map(str, r)))), rows))+'\n')

def writepickle(tracks, fpath=os.path.join(output_dir, 'tracks.pickle')):
    with open(fpath, 'wb') as f:
        for track in tracks:
            pickle.dump(track, f)

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



def graph_blocking_io(fpath, domain):
    from merge_data import merge_layers
    for x in merge_layers(trackgen(deserialize_generator(fpath))):
        yield x

def graph_cpu_bound(track, domain, **params):
    #timesplit = partial(segment_tracks_timesplits, maxdelta=cuttime)
    distsplit = partial(segment_tracks_encode_greatcircledistance, **params)
    geofenced = partial(fence_tracks,               domain=domain)
    #split_len = partial(max_tracklength,              max_track_length=10000)
    serialize = partial(serialize_network_edge,     domain=domain)
    print('processing mmsi', track['mmsi'], end='\r')
    #list(serialize(geofenced(split_len(distsplit(timesplit([track]))))))
    #for t in serialize(geofenced(distsplit([track]))):
    #    pass
    list(serialize(geofenced(distsplit([track]))))
    #return


def graph(fpath, domain, parallel=0, **params):
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
        for track in graph_blocking_io(fpath, domain):
            graph_cpu_bound(track, domain=domain, **params)
        print()

    else:
        with Pool(processes=parallel) as p:
            fcn = partial(graph_cpu_bound, domain=domain, **params)
            p.imap_unordered(fcn, (tr for tr in graph_blocking_io(fpath, domain=domain)), chunksize=1)
            p.close()
            p.join()
        print()


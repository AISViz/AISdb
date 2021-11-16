import os
import zipfile
from multiprocessing import Pool
from functools import partial
from datetime import datetime, timedelta
import pickle

import numpy as np

from network_graph import serialize_network_edge
from merge_data import merge_layers
from track_gen import trackgen, segment_tracks_timesplits, segment_tracks_dbscan, fence_tracks, concat_tracks, segment_tracks_encode_greatcircledistance

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



def blocking_io(fpath, domain):
    for x in merge_layers(trackgen(deserialize_generator(fpath))):
        yield x

def cpu_bound(track, domain):
    timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=2))
    #distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=50)
    distsplit = partial(segment_tracks_encode_greatcircledistance, distance_meters=125000)
    geofenced = partial(fence_tracks,               domain=domain)
    split_len = partial(concat_tracks,              max_track_length=10000)
    serialize = partial(serialize_network_edge,     domain=domain)
    print(track['mmsi'], end='\r')
    list(serialize(geofenced(distsplit(split_len(timesplit([track]))))))
    return


def graph(fpath, domain, parallel=0):
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
    '''
    filtering = partial(filter_tracks,              filter_callback=lambda track: (
                                                    len(track['time']) <= 2 
                                                    #or track['hourly_transits_avg'] > 6
                                                    or set(track['in_zone']) == {'Z0'}
                                                    or np.max(delta_knots(track, np.array(range(len(track['time']))))) > 50
                                                    ),
                                                    logging_callback=lambda track: (
                                                    #track['hourly_transits_avg'] > 6 or 
                                                    not (len(track['time']) > 1
                                                    and np.max(delta_knots(track, np.array(range(len(track['time']))))) > 50)
                                                    ),)
    '''
    if not parallel: 
        #for fpath in fpaths:
            #geofence(track, domain=domain)
        for track in blocking_io(fpath, domain):
            cpu_bound(track, domain=domain)

    else:
        with Pool(processes=parallel) as p:
            #fcn = partial(geofence, domain=domain)
            #p.map(fcn, (list(m) for m in tracks), chunksize=1)  # better tracebacks for debug
            fcn = partial(cpu_bound, domain=domain)
            p.imap_unordered(fcn, (tr for tr in blocking_io(fpath, domain=domain)), chunksize=1)
            #print(results[0])
            #print(list(results))
            p.close()
            p.join()


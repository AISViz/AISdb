import os
import zipfile
from multiprocessing import Pool
from functools import partial, reduce
from datetime import datetime, timedelta
import pickle

import numpy as np

from common import output_dir


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
    low, high = 0, arr.size - 1
    if descending:
        arr = arr[::-1]
    while (low <= high):
        mid = (low + high) // 2
        if search >= arr[mid - 1] and search <= arr[mid + 1]:
            break
        elif (arr[mid] > search):
            high = mid - 1
        else:
            low = mid + 1
    if descending:
        return arr.size - mid - 1
    else:
        return mid


def writecsv(rows, pathname='/data/smith6/ais/scripts/output.csv', mode='a'):
    with open(pathname, mode) as f:
        f.write('\n'.join(
            map(
                lambda r: ','.join(
                    map(lambda r: r.replace(',', '').replace('#', ''),
                        map(str.rstrip, map(str, r)))), rows)) + '\n')


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
                with open(os.path.join(tmp_dir, '__' + str(rows[0][0])),
                          'wb') as f2:
                    pickle.dump(rows, f2)
            except EOFError as e:
                break


def deserialize(fpaths):
    for fpath in fpaths:
        assert isinstance(fpath, str), f'fpath = {list(fpath)}'
        print('processing ', fpath)
        with open(fpath, 'rb') as f:
            yield pickle.load(f)


def glob_files(dirpath, ext='.txt', keyorder=lambda key: key):
    ''' walk a directory to glob txt files. can be used with ZoneGeomFromTxt()

        zones_dir: string
            directory to walk
        keyorder:
            anonymous function for custom sort ordering

        example keyorder:

        .. code-block::

            # numeric sort on zone names with strsplit on 'Z' char
            keyorder=lambda key: int(key.rsplit(os.path.sep, 1)[1].split('.')[0].split('Z')[1])

        returns:
            .txt shapefile paths

    '''
    #txtpaths = reduce(np.append,
    #    [list(map(os.path.join, (path[0] for p in path[2]), path[2])) for path in list(os.walk(zones_dir))]
    #)

    paths = list(os.walk(dirpath))

    extfiles = [[
        p[0],
        sorted([f for f in p[2] if f[-len(ext):] == ext], key=keyorder)
    ] for p in paths if len(p[2]) > 0]

    extpaths = reduce(np.append, [
        list(map(os.path.join, (path[0] for p in path[1]), path[1]))
        for path in extfiles
    ], np.array([], dtype=object))

    return sorted(extpaths, key=keyorder)


from shapely.geometry import Point, MultiPoint, LineString


def serialize_geomwkb(tracks):
    ''' for each track dictionary, serialize the geometry as WKB to the output directory '''
    wkbdir = os.path.join(output_dir, 'wkb/')
    if not os.path.isdir(wkbdir):
        os.mkdir(wkbdir)

    for track in tracks:
        if len(track['time']) == 1:
            geom = MultiPoint([
                Point(x, y, t)
                for x, y, t in zip(track['lon'], track['lat'], track['time'])
            ])
        else:
            geom = LineString(zip(track['lon'], track['lat'], track['time']))
        fname = os.path.join(
            wkbdir,
            f'mmsi={track["mmsi"]}_epoch={int(track["time"][0])}-{int(track["time"][-1])}_{geom.type}.wkb'
        )
        with open(fname, 'wb') as f:
            f.write(geom.wkb)

    return


'''
def cpu_bound(track, domain, cutdistance, maxdistance, cuttime, minscore):
    timesplit = partial(segment_tracks_timesplits, maxdelta=cuttime)
    distsplit = partial(segment_tracks_encode_greatcircledistance,
                        cutdistance=cutdistance,
                        maxdistance=maxdistance,
                        cuttime=cuttime,
                        minscore=minscore)
    geofenced = partial(fence_tracks, domain=domain)
    split_len = partial(max_tracklength, max_track_length=10000)
    print('processing mmsi', track['mmsi'], end='\r')
    serialize_geomwkb(split_len(distsplit(timesplit([track]))))
    return


def serialize_geoms(tracks,
                    domain,
                    processes,
                    cutdistance=5000,
                    maxdistance=125000,
                    cuttime=timedelta(hours=6),
                    minscore=0.0001):
    with Pool(processes=processes) as p:
        fcn = partial(cpu_bound,
                      domain=domain,
                      cutdistance=cutdistance,
                      maxdistance=maxdistance,
                      cuttime=cuttime,
                      minscore=minscore)
        p.imap_unordered(fcn, tracks, chunksize=1)
        p.close()
        p.join()
    print()
'''

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

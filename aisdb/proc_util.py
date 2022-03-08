import os
import zipfile
from multiprocessing import Pool
from functools import partial, reduce
from datetime import datetime, timedelta
import pickle
import re

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
    ''' fast indexing of ordered arrays

        caution: will return nearest index in out-of-bounds cases
    '''
    low, high = 0, arr.size - 1
    if descending:
        arr = arr[::-1]
    if search < arr[0]:
        return 0
    elif search >= arr[-1]:
        return len(arr) - 1
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


def _segment_rng(track: dict, maxdelta: timedelta, minsize: int) -> filter:
    assert isinstance(track, dict), f'wrong track type {type(track)}:\n{track}'
    splits_idx = lambda track: np.append(
        np.append([0],
                  np.nonzero(track['time'][1:] - track['time'][:-1] >= maxdelta
                             .total_seconds() / 60)[0] + 1),
        [track['time'].size])
    return filter(
        lambda seg: len(seg) >= minsize,
        list(map(range,
                 splits_idx(track)[:-1],
                 splits_idx(track)[1:])))


def writecsv(rows, pathname='/data/smith6/ais/scripts/output.csv', mode='a'):
    with open(pathname, mode) as f:
        f.write('\n'.join(
            map(
                lambda r: ','.join(
                    map(lambda r: r.replace(',', '').replace('#', ''),
                        map(str.rstrip, map(str, r)))), rows)) + '\n')


def writebinary(tracks, fpath=os.path.join(output_dir, 'tracks.vec')):
    with open(fpath, 'wb') as f:
        for track in tracks:
            pickle.dump(track, f)


def readbinary(fpath=os.path.join(output_dir, 'tracks.vec'), count=None):
    results = []
    n = 0
    with open(fpath, 'rb') as f:
        while True:
            try:
                getrow = pickle.load(f)
            except EOFError:
                break
            except Exception as e:
                raise e
            n += 1
            results.append(getrow)
            if count is not None and n >= count:
                break

    return results


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


def datefcn(fpath):
    return re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}').search(fpath)


def regexdate_2_dt(reg, fmt='%Y%m%d'):
    return datetime.strptime(reg.string[reg.start():reg.end()], fmt)


def getfiledate(fpath, fmt='%Y%m%d'):
    d = datefcn(fpath)
    if d is None:
        print(f'warning: could not parse YYYYmmdd format date from {fpath}!')
        print('warning: defaulting to epoch zero!')
        return datetime(1970, 1, 1)
    fdate = regexdate_2_dt(d, fmt=fmt)
    return fdate

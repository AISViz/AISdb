import os
import zipfile
from multiprocessing import Pool
from functools import partial, reduce
from datetime import datetime, timedelta
import pickle
import re
import csv

import numpy as np


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


def _splits_idx(vector: np.ndarray, d: timedelta) -> np.ndarray:
    if isinstance(d, timedelta):
        splits = np.nonzero(
            vector[1:] - vector[:-1] >= d.total_seconds())[0] + 1
    else:
        splits = np.nonzero(vector[1:] - vector[:-1] >= d)[0] + 1
    idx = np.append(np.append([0], splits), [vector.size])
    return idx


def _segment_rng(track, maxdelta, key='time') -> filter:
    ''' index time segments '''
    for rng in map(
            range,
            _splits_idx(track[key], maxdelta)[:-1],
            _splits_idx(track[key], maxdelta)[1:],
    ):
        yield rng


def write_csv_rows(rows,
                   pathname='/data/smith6/ais/scripts/output.csv',
                   mode='a'):
    with open(pathname, mode) as f:
        f.write('\n'.join(
            map(
                lambda r: ','.join(
                    map(lambda r: r.replace(',', '').replace('#', ''),
                        map(str.rstrip, map(str, r)))), rows)) + '\n')


'''
def _datetime_column(tracks):
    for track in tracks:
        track['datetime'] = np.array(
            epoch_2_dt(track['time'].astype(int)),
            dtype=object,
        )
        track['dynamic'] = track['dynamic'].union(set(['datetime']))
        yield track
'''


def write_csv(
    tracks,
    fpath,
    skipcols=['mmsi', 'label', 'in_zone', 'ship_type'],
):

    cols = [
        'mmsi', 'time', 'datetime', 'lon', 'lat', 'vessel_name',
        'ship_type_txt', 'imo', 'dim_bow', 'dim_stern', 'dim_star', 'dim_port'
    ]
    assert False, 'datetime column function undefined'
    tracks_dt = _datetime_column(tracks)

    tr1 = next(tracks_dt)

    colnames = (
        cols + [f for f in tr1['dynamic'] if f not in cols + skipcols] +
        [f for f in list(tr1['static'])[::-1] if f not in cols + skipcols])

    decimals = {
        'lon': 5,
        'lat': 5,
        'depth_metres': 2,
        'distance_metres': 2,
        'submerged_hull_m^2': 0,
    }

    def _append(track, writer, colnames=colnames, decimals=decimals):
        for i in range(0, track['time'].size):
            row = [(track[c][i] if c in track['dynamic'] else
                    (track[c] if track[c] != 0 else '')) for c in colnames]
            for ci, r in zip(range(len(colnames)), row):
                if colnames[ci] in decimals.keys() and r != '':
                    row[ci] = f'{r:.{decimals[colnames[ci]]}f}'

            writer.writerow(row)

    with open(fpath, 'w', newline='') as f:
        f.write(','.join(colnames) + '\n')
        writer = csv.writer(f,
                            delimiter=',',
                            quoting=csv.QUOTE_NONE,
                            dialect='unix')
        _append(tr1, writer, colnames, decimals)
        for track in tracks_dt:
            _append(track, writer, colnames, decimals)

    return


def write_binary(tracks, fpath):
    with open(fpath, 'wb') as f:
        for track in tracks:
            pickle.dump(track, f)


def read_binary(fpath, count=None):
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


def dms2dd(d, m, s, ax):
    ''' convert degrees, minutes, seconds to decimal degrees '''
    dd = float(d) + float(m) / 60 + float(s) / (60 * 60)
    if (ax == 'W' or ax == 'S') and dd > 0: dd *= -1
    return dd


def strdms2dd(strdms):
    '''  convert string representation of degrees, minutes, seconds to decimal deg '''
    d, m, s, ax = [v for v in strdms.replace("''", '"').split(' ') if v != '']
    return dms2dd(float(d.rstrip('°')), float(m.rstrip("'")),
                  float(s.rstrip('"')), ax.upper())

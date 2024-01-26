from datetime import datetime, timedelta
from functools import partial, reduce
from tempfile import SpooledTemporaryFile
import csv
import io
import os
import re
import typing

import numpy as np


def _sanitize(s):
    # note: the first comma uses ASCII code 44,
    # second comma uses ASCII decimal 130 !!
    # not the same char!
    if s is None:
        return ''
    elif s == '-':
        return ''
    else:
        return str(s).replace(',', '').replace(chr(130), '').replace(
            '#', '').replace('"', '').replace("'", '').replace('\n', '')


def _epoch_2_dt(ep_arr, t0=datetime(1970, 1, 1, 0, 0, 0), unit='seconds'):
    ''' convert epoch minutes to datetime.datetime.
        redefinition of function in aisdb.gis to avoid circular import
    '''

    delta = lambda ep, unit: t0 + timedelta(**{unit: ep})

    if isinstance(ep_arr, (list, np.ndarray)):
        return np.array(list(map(partial(delta, unit=unit), map(int, ep_arr))))

    elif isinstance(ep_arr, (float, int, np.uint32)):
        return delta(int(ep_arr), unit=unit)

    else:  # pragma: no cover
        raise ValueError(
            f'input must be integer or array of integers. got {ep_arr=}{type(ep_arr)}'
        )


def _splits_idx(vector: np.ndarray, d: timedelta) -> np.ndarray:
    assert isinstance(d, timedelta)
    vector = np.array(vector, dtype=int)
    splits = np.nonzero(vector[1:] - vector[:-1] >= d.total_seconds())[0] + 1
    #else:
    #    splits = np.nonzero(vector[1:] - vector[:-1] >= d)[0] + 1
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


def _datetime_column(tracks):
    for track in tracks:
        assert isinstance(track, dict), f'got {track=}'
        track['datetime'] = np.array(
            _epoch_2_dt(track['time'].astype(int)),
            dtype=object,
        )
        track['dynamic'] = track['dynamic'].union(set(['datetime']))
        yield track


_columns_order = [
    'mmsi', 'imo', 'vessel_name', 'name', 'datetime', 'time', 'lon', 'lat',
    'cog', 'sog', 'dim_bow', 'dim_stern', 'dim_star', 'dim_port',
    'coarse_type_txt', 'vesseltype_generic', 'vesseltype_detailed', 'callsign',
    'flag', 'gross_tonnage', 'summer_dwt', 'length_breadth', 'year_built',
    'home_port', 'error404'
]


def tracks_csv(tracks, skipcols: list = ['label', 'in_zone']):
    ''' Yields row tuples when given a track generator.
        See write_csv() for more info
    '''
    tracks_dt = _datetime_column(tracks)
    track_ID = 1
    tr1 = next(tracks_dt)
    colnames = [
        c for c in _columns_order + list(
            set(tr1['static'].union(tr1['dynamic'])) -
            set(_columns_order).union(set(['marinetraffic_info'])))
        if c in list(tr1['static']) + list(tr1['dynamic'])
    ]
    colnames = [col for col in colnames if col not in skipcols]

    yield colnames  +["Track_ID"]

    if 'marinetraffic_info' in tr1.keys():
        colnames += tuple(tr1['marinetraffic_info'].keys())
        colnames.remove('error404')
        colnames.remove('dim_bow')
        colnames.remove('dim_stern')
        colnames.remove('dim_star')
        colnames.remove('dim_port')
        if 'coarse_type_txt' in colnames:  # pragma: no cover
            colnames.remove('coarse_type_txt')
        if 'vessel_name' in colnames:  # pragma: no cover
            colnames.remove('vessel_name')
        colnames = list(dict.fromkeys(colnames))

    decimals = {
        'lon': 5,
        'lat': 5,
        'depth_metres': 2,
        'distance_metres': 2,
        'submerged_hull_m^2': 0,
    }

    def _append(track, colnames=colnames, decimals=decimals, track_id= 0):
        if 'marinetraffic_info' in track.keys():
            for key, val in dict(track['marinetraffic_info']).items():
                if key in ('error404', 'mmsi', 'imo'):
                    continue
                track[key] = val
            del track['marinetraffic_info']

        for i in range(0, track['time'].size):
            row = [(track[c][i] if c in track['dynamic'] else
                    (_sanitize(track[c]) if track[c] != 0 else ''))
                   for c in colnames]
            for ci, r in zip(range(len(colnames)), row):
                if colnames[ci] in decimals.keys() and r != '':
                    row[ci] = f'{float(r):.{decimals[colnames[ci]]}f}'

            row.append(track_id)
            # writer.writerow(row)
            yield row

    yield from _append(tr1, colnames, decimals, track_id=track_ID)
    for track in tracks_dt:
        track_ID+=1
        yield from _append(track, colnames, decimals, track_id=track_ID)


def write_csv(
    tracks,
    fpath: typing.Union[io.BytesIO, str, SpooledTemporaryFile],
    skipcols: list = ['label', 'in_zone'],
):
    ''' write track vector dictionaries as CSV file

        args:
            tracks (iter)
                track generator such as returned by
                :func:`aisdb.track_gen.TrackGen`
            fpath (string)
                output CSV filepath (string) or io.BytesIO buffer
            skipcols (list)
                columns to be omitted from results
    '''

    #with open(fpath, 'w', newline='') as f:
    if isinstance(fpath, str):
        f = open(fpath, mode='w')
    elif isinstance(fpath, (io.BytesIO, SpooledTemporaryFile)):
        f = io.TextIOWrapper(fpath, encoding='utf8', newline='')
    else:
        raise ValueError(f'invalid type for fpath: {type(fpath)}')

    #with f:
    #f.write(','.join(colnames) + '\n')
    writer = csv.writer(f,
                        delimiter=',',
                        quotechar="'",
                        quoting=csv.QUOTE_NONE,
                        dialect='unix')
    for row in tracks_csv(tracks):
        writer.writerow(row)

    if isinstance(fpath, str):
        f.close()
    else:
        # prevent bytesIO buf from being cleaned up with TextIOWrapper
        f.detach()

    return


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


def getfiledate(filename):
    ''' attempt to parse the first valid epoch timestamp from .nm4 data file.
        timestamp will be returned as :class:`datetime.date` if successful,
        otherwise will return False if no date could be found

        args:
            filename (string)
                raw AIS data file in .nm4 format
    '''
    filesize = os.path.getsize(filename)
    if filesize == 0:  # pragma: no cover
        return False
    with open(filename, 'r') as f:
        if filename.lower()[-3:] == "csv":
            reader = csv.reader(f)
            head = next(reader)
            row1 = next(reader)
            rowdict = {a: b for a, b in zip(head, row1)}
            fdate = datetime.strptime(rowdict['Time'], '%Y%m%d_%H%M%S').date()
            return fdate
        else:
            line = f.readline()
            head = line.rsplit('\\', 1)[0]
            n = 0
            while 'c:' not in head:  # pragma: no cover
                n += 1
                line = f.readline()
                head = line.rsplit('\\', 1)[0]
                #if n > 10000:
                #    print(f'bad! {filename}')
                #    return False
                assert n <= 10000
            split0 = re.split('c:', head)[1]
            try:
                epoch = int(re.split('[^0-9]', split0)[0])
            except ValueError:  # pragma: no cover
                return False
            except Exception as err:  # pragma: no cover
                raise err
        fdate = datetime.fromtimestamp(epoch).date()
        return fdate

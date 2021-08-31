from functools import reduce
from datetime import timedelta


from database import epoch_2_dt

import numpy as np

def trackgen(
        rows: np.ndarray,
        colnames: list = [
            'mmsi', 'time', 'lon', 'lat',
            'cog', 'sog', 'msgtype',
            'imo', 'vessel_name',
            'dim_bow', 'dim_stern', 'dim_port', 'dim_star',
            'ship_type', 'ship_type_txt', ],
        deduplicate_timestamps: bool = True,
        ) -> dict:
    ''' each row contains columns from database: 
            mmsi time lon lat cog sog name type...
        rows must be sorted by first by mmsi, then time

        colnames is the name associated with each column type in rows. 
        first two columns must be ['mmsi', 'time']
    '''
    mmsi_col = [i for i,c in zip(range(len(colnames)), colnames) if c.lower() == 'mmsi'][0]
    time_col = [i for i,c in zip(range(len(colnames)), colnames) if c.lower() == 'time'][0]

    if deduplicate_timestamps:
        dupe_idx = np.nonzero(rows[:,time_col].astype(int)[:-1] == rows[:,time_col].astype(int)[1:])[0] +1
        rows = np.delete(rows, dupe_idx, axis=0)
        
    staticcols = set(colnames) & set([
        'vessel_name', 'ship_type', 'ship_type_txt', 'dim_bow', 'dim_stern', 'dim_port', 'dim_star', 
        'mother_ship_mmsi', 'part_number', 'vendor_id', 'model', 'serial', 'imo', 'msgtype',
        'deadweight_tonnage', 'submerged_hull_m^2',
    ])

    dynamiccols = set(colnames) - staticcols - set(['mmsi', 'time'])

    tracks_idx = np.append(np.append([0], np.nonzero(rows[:,mmsi_col].astype(int)[1:] != rows[:,mmsi_col].astype(int)[:-1])[0]+1), len(rows))

    for i in range(len(tracks_idx)-1): 
        #assert len(rows[tracks_idx[i]:tracks_idx[i+1]].T[1]) == len(np.unique(rows[tracks_idx[i]:tracks_idx[i+1]].T[1]))
        yield dict(
            mmsi    =   int(rows[tracks_idx[i]][0]),
            time    =   rows[tracks_idx[i]:tracks_idx[i+1]].T[1],
            static  =   staticcols,
            dynamic =   dynamiccols,
            **{ n   :   (rows[tracks_idx[i]][c] or 0) 
                    for c,n in zip(range(2, len(colnames)), colnames[2:]) if n in staticcols},
            **{ n   :   rows[tracks_idx[i]:tracks_idx[i+1]].T[c] 
                    for c,n in zip(range(2, len(colnames)), colnames[2:]) if n in dynamiccols},
        )


#def segment(track: dict, maxdelta: timedelta, minsize: int) -> filter:
#    splits_idx = lambda track: np.append(np.append([0], np.nonzero(track['time'][1:] - track['time'][:-1] >= maxdelta)[0]+1), [len(track['time'])])
#    return filter(lambda seg: len(seg) >= minsize, list(map(range, splits_idx(track)[:-1], splits_idx(track)[1:])))

def segment(track: dict, maxdelta: timedelta, minsize: int) -> filter:
    splits_idx = lambda track: np.append(np.append([0], np.nonzero(track['time'][1:] - track['time'][:-1] >= maxdelta.total_seconds() / 60 )[0]+1), [len(track['time'])])
    return filter(lambda seg: len(seg) >= minsize, list(map(range, splits_idx(track)[:-1], splits_idx(track)[1:])))


def filtermask(track, rng, filters, first_val=False):
    '''
    from .gis import compute_knots
    filters=[
            lambda track, rng: track['time'][rng][:-1] != track['time'][rng][1:],
            #lambda track, rng: compute_knots(track, rng) < 40,
            lambda track, rng: (compute_knots(track, rng[:-1]) < 40) & (compute_knots(track, rng[1:]),
            lambda track, rng: np.full(len(rng)-1, 201000000 <= track['mmsi'] < 776000000, dtype=np.bool), 
        ]
    '''
    mask = reduce(np.logical_and, map(lambda f: f(track, rng), filters))
    #return np.logical_and(np.append([True], mask), np.append(mask, [True]))
    return np.append([first_val], mask)


def writecsv(rows, pathname='/data/smith6/ais/scripts/output.csv', mode='a'):
    with open(pathname, mode) as f: 
        f.write('\n'.join(map(lambda r: ','.join(map(lambda r: r.replace(',','').replace('#',''), map(str.rstrip, map(str, r)))), rows))+'\n')


def readcsv(pathname='/data/smith6/ais/scripts/output.csv', header=True):
    with open(pathname, 'r') as csvfile: 
        reader = csv.reader(csvfile, delimiter=',')
        rows = np.array(list(reader), dtype=object)
        if header: columns, rows = rows[0], rows[1:]
    return np.array([c.astype(t) for c,t in zip(rows.T, 
        [np.uint32, str, str, str, np.float32, np.float32, np.float32, np.float32, np.float32, str, str, str, str, str]
        )], dtype=object).T


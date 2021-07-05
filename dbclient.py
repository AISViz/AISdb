import os
from multiprocessing import Pool#, set_start_method
import pickle
import time
from functools import partial
from datetime import datetime, timedelta

import numpy as np
from shapely.geometry import Point, LineString, Polygon


from gis import compute_knots
from database import dt2monthstr, dbconn
from track_gen import trackgen, segment, filtermask, writecsv
from shore_dist import shore_dist_gfw
from webdata import marinetraffic 
from webdata.marinetraffic import scrape_tonnage
from gebco import Gebco
from wsa import wsa



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


def _mergeprocess(track, zones, tmpdir, colnames):
    ''' parallel process function for segmenting and geofencing tracks
        appends columns for bathymetry, shore dist, and hull surface area
    '''

    print(track['mmsi'])
    chunksize=5000
    filters = [
            lambda track, rng: compute_knots(track, rng) < 50,
        ]

    filepath = os.path.join(tmpdir, str(track['mmsi']))
    '''
    bounds_lon, bounds_lat = zones['hull_xy'][::2], zones['hull_xy'][1::2]
    west, east = np.min(bounds_lon), np.max(bounds_lon)
    south, north = np.min(bounds_lat), np.max(bounds_lat)

    assert north <= 90
    assert south >= -90
    if west < -180 or east > 180:
        print('warning: encountered longitude boundary exceeding (-180, 180) in zone geometry')
        if west < -180: west = -180
        if east > 180: east = 180
    '''

    with open(filepath, 'ab') as f:

        for rng in segment(track, maxdelta=timedelta(hours=2), minsize=3):

            mask = filtermask(track, rng, filters)

            if (n := sum(mask)) == 0: continue
            for c in range(0, (n // chunksize) + 1, chunksize):

                nc = c + chunksize
                track_xy = list(zip(track['lon'][rng][mask][c:nc], track['lat'][rng][mask][c:nc]))

                # get subset of zones that intersect with track
                in_zones = { k:v for k,v in zones['geoms'].items() if LineString(track_xy).intersects(v) }

                # from these zones, get zone for individual points
                zoneID = (([k for k,v in in_zones.items() if v.contains(p)] or [None])[0] for p in map(Point, track_xy))
                
                # append zone context to track rows
                stacked = np.vstack((
                        [track['mmsi'] for _ in range(sum(mask[c:nc]))],
                        track['time'][rng][mask][c:nc],
                        *(np.array([track[col] for _ in range(n)])[c:nc] for col in colnames if col in track['static']),  # static columns - msgs 5, 24
                        *(track[col][rng][mask][c:nc] for col in colnames if col in track['dynamic']),  # dynamic columns - msgs 1, 2, 3, 18 
                        np.append(compute_knots(track, rng), [0])[mask][c:nc],  # computed sog
                        list(zoneID),
                        np.array([zones['domain'] for _ in range(n)])[c:nc],
                    )).T

                source_stats = dict(
                    
                    )


                pickle.dump(out, f)

    return True


def merge_layers(rows, zones, dbpath):
    """ # set start method in main script
        import os; from multiprocessing import set_start_method
        if os.name == 'posix' and __name__ == '__main__': 
            set_start_method('forkserver')
    """
    # create temporary directory for parsed data
    path, dbfile = dbpath.rsplit(os.path.sep, 1)
    tmpdir = os.path.join(path, 'tmp_parsing')
    if not os.path.isdir(tmpdir):
        os.mkdir(tmpdir)

    # read data layers from disk to merge with AIS
    print('aggregating ais, shore distance, bathymetry, vessel geometry...')
    with (  shore_dist_gfw() as sdist,
            Gebco() as bathymetry, 
            marinetraffic.scrape_tonnage(dbpath) as hullgeom    ):

        xy = rows[:,2:4]
        mmsi_column, imo_column, ship_type_column = 0, 7, 13

        # vessel geometry
        print('loading marinetraffic vessel data...')
        deadweight_tonnage = np.array([hullgeom.get_tonnage_mmsi_imo(r[mmsi_column], r[imo_column] or 0 ) if r[imo_column] != None else 0  for r in rows ])

        # wetted surface area - regression on tonnage and ship type
        ship_type = np.logical_or(rows[:,ship_type_column], [0 for _ in range(len(rows))])
        submerged_hull = np.array([wsa(d, r) for d,r in zip(deadweight_tonnage,ship_type) ])

        # shore distance from cell grid
        print('loading shore distance...')
        km_from_shore = np.array([sdist.getdist(x, y) for x, y in xy ])

        # seafloor depth from cell grid
        print('loading bathymetry...')
        depth = np.array([bathymetry.getdepth(x, y) for x,y in xy ]) * -1

    merged = np.hstack((rows, np.vstack((deadweight_tonnage, submerged_hull, km_from_shore, depth)).T))

    colnames = [
        'mmsi', 'time', 'lon', 'lat', 
        'cog', 'sog', 'msgtype',
        'imo', 'vessel_name',  
        'dim_bow', 'dim_stern', 'dim_port', 'dim_star',
        'ship_type', 'ship_type_txt',
        'deadweight_tonnage', 'submerged_hull_m^2',
        'depth_metres',
    ]


    print('merging...')

    with Pool(processes=12) as p:
        # define fcn as _mergeprocess() with zones, db context as static args
        fcn = partial(_mergeprocess, zones=zones, tmpdir=tmpdir, colnames=colnames)
        # map track generator to anonymous fcn for each process in processing pool
        p.imap_unordered(fcn, trackgen(merged, colnames=colnames), chunksize=1)
        p.close()
        p.join()
    _ = [colnames.append(col) for col in ['sog_computed', 'zone', 'domain']]

    csvfile = path + os.path.sep + 'output.csv'
    with open(csvfile, 'w') as f: 
        f.write(', '.join(colnames) +'\n')

    for picklefile in sorted(os.listdir(tmpdir)):
        results = np.ndarray(shape=(0, len(colnames)) )
        with open(os.path.join(tmpdir, picklefile), 'rb') as f:
            while True:
                try:
                    getrows = pickle.load(f)
                except EOFError as e:
                    break
                except Exception as e:
                    raise e
                results = np.vstack((results, getrows))
        writecsv(results, pathname=csvfile)
        os.remove(picklefile)

    return 




'''
res = next(getrows(qryfcn, rows_months, months_str, cols))
gen = explode_month(*next(getrows(qryfcn,rows_months,months_str), cols, csvfile))

'''
"""
if __name__ == '__main__':
    for track in trackgen(rows):
        for rng in segment(track, maxdelta=timedelta(hours=2), minsize=3):
            mask = filtermask(track, rng)
            sog_haversine = compute_knots(track, rng)
            print(f'{track["mmsi"]}  rng: {rng}  sog vs haversine avg diff: '
                  f'{np.average(np.abs(np.append([0],sog_haversine)[mask] - track["sog"][rng][mask]))}')
"""


def getrows(conn, qryfcn, rows_months, months_str, cols):
    '''
    qrows_month, mstr = rows_months[-1], months_str[-1]
    '''
    #callback = lambda alias, ummsi, **_: f'''{alias}.mmsi in ('{"', '".join(map(str, ummsi))}')'''
    callback = lambda alias, ummsi, **_: f'''{alias}.mmsi in ({", ".join(map(str, ummsi))})'''
    for qrows_month, mstr in zip(rows_months, months_str):
        print(f'querying {mstr}...')
        ummsi = np.unique(qrows_month[:,cols['mmsi']])
        qry = qryfcn(mstr, callback, dict(ummsi=ummsi)) + '\nORDER BY mmsi, time'
        cur = conn.cursor()
        cur.execute(qry)
        res = np.array(cur.fetchall())
        #logging.debug(f'{np.unique(res[:,0])}')
        yield dict(res=res, qrows_month=qrows_month, cols=cols, mstr=mstr)


def explode_month(kwargs, csvfile, keepraw=True):
    '''
    kwargs = list(getrows(qryfcn,rows_months, months_str, cols))[-1]
    '''
    qrows_month, res, mstr, cols = kwargs['qrows_month'], kwargs['res'], kwargs['mstr'], kwargs['cols']
    print(f'exploding rowdata from {mstr}...')
    tracks = { t['mmsi'] : t for t in trackgen(res) }

    raw = np.ndarray(shape=(0, len(cols['keepcols']) + 8), dtype=object)
    out = np.ndarray(shape=(0, len(cols['keepcols']) + 9), dtype=object)
    for qrow in qrows_month:
        if (mmsi := int(qrow[cols['mmsi']])) not in tracks.keys(): continue
        if len(tracks[mmsi]['time']) == 0: continue
        mask = ((tracks[mmsi]['time'] > qrow[cols['start']]) * (tracks[mmsi]['time'] < qrow[cols['end']]))
        n = sum(mask)
        if n == 0: continue
        track = tracks[mmsi].copy()
        for c in ['time', 'lon', 'lat', 'cog', 'sog']: track[c] = track[c][mask] 
        if keepraw: raw = np.vstack((raw, 
            np.vstack((
                    *(np.array([qrow[c] for _ in range(n)]) for c in cols['keepcols']),
                    np.array([track['mmsi'] for _ in range(n)], dtype=np.uint32), # mmsi
                    np.array([track['vessel_name'] for _ in range(n)]), # name 
                    np.array([track['ship_type_txt'] for _ in range(n)]), # type
                    track['time'],
                    track['lon'],
                    track['lat'],
                    track['cog'],
                    track['sog'],
                )).T
            ))
        for rng in segment(track, timedelta(days=7), minsize=1):
            mask2 = filtermask(track, rng, filters=[lambda track, rng: compute_knots(track, rng) < 50])
            n2 = sum(mask2)
            out = np.vstack((out, 
                    np.vstack((
                        *(np.array([qrow[c] for _ in range(n2)]) for c in cols['keepcols']),
                        np.array([track['mmsi'] for _ in range(n2)], dtype=np.uint32), # mmsi
                        np.array([track['vessel_name'] for _ in range(n2)]), # name 
                        np.array([track['ship_type_txt'] for _ in range(n2)]), # type
                        track['time'][rng][mask2],
                        track['lon'][rng][mask2],
                        track['lat'][rng][mask2],
                        track['cog'][rng][mask2],
                        track['sog'][rng][mask2],
                        np.append(compute_knots(track, rng), [0]).astype(np.uint16)[mask2],
                    )).T
                ))
                    
    assert len(out) > 0

    print(f'mmsis found: {len(np.unique(raw[:,3]))} / {len(np.unique(qrows_month[:,3]))}')

    if keepraw: writecsv(raw, csvfile + f'.raw.{mstr}', mode='a',)
    writecsv(out, csvfile + f'.filtered.{mstr}', mode='a',)
    

def explode(conn, qryfcn, qryrows, cols, dateformat='%m/%d/%Y', csvfile='output/test.csv'):
    '''
        crawl database for rows with matching mmsi + time range

        cols = {'mmsi':3, 'start':-2, 'end':-1, 'sort_output':1, 'keepcols':[0,1,2]}
 
        
    '''
    print('building index...')

    # sort by mmsi, time
    qrows = qryrows[qryrows[:,cols['mmsi']].argsort()]
    dt = np.array(list(map(lambda t: datetime.strptime(t, dateformat), qrows[:,cols['start']])))
    qrows = qrows[dt.argsort(kind='mergesort')]
    dt = np.array(list(map(lambda t: datetime.strptime(t, dateformat), qrows[:,cols['start']])))
    dt2 = np.array(list(map(lambda t: datetime.strptime(t, dateformat), qrows[:,cols['end']])))

    # filter timestamps outside data range
    exists = np.logical_or( 
            ((datetime(2011, 1, 1) < dt) & (dt < datetime(datetime.today().year, datetime.today().month, 1))),
            ((datetime(2011, 1, 1) < dt2) & (dt2 < datetime(datetime.today().year, datetime.today().month, 1)))
        )
    qrows = qrows[exists]
    dt = dt[exists]
    dt2 = dt2[exists]

    # filter bad mmsi
    hasmmsi = np.array(list(map(str.isnumeric, qrows[:,cols['mmsi']])))
    qrows = qrows[hasmmsi]
    dt = dt[hasmmsi]
    dt2 = dt2[hasmmsi]
    validmmsi = np.logical_and((m := qrows[:,cols['mmsi']].astype(int)) >= 201000000, m < 776000000)
    qrows = qrows[validmmsi]
    dt = dt[validmmsi]
    dt2 = dt2[validmmsi]
    qrows[:,cols['start']] = dt
    qrows[:,cols['end']] = dt2

    # aggregate by month
    #months_str = dt2monthstr(dt2[0], dt[-1])
    months_str = dt2monthstr(dt[0], dt2[-1])
    months = np.array([datetime.strptime(m, '%Y%m') for m in months_str])
    months = np.append(months, months[-1] + timedelta(days=31))
    rows_months = [ qrows[((dt > m1) & (dt < m2)) | ((dt2 > m1) & (dt2 < m2))] for m1, m2 in zip(months[:-1], months[1:]) ]

    '''
    kwargs = next(getrows(qryfcn,rows_months[-1:],months_str[-1:], cols))
    explode_month(kwargs, csvfile)
    '''

    '''
    with Pool(processes=3) as p:
        p.imap_unordered(partial(explode_month, csvfile=csvfile), getrows(conn, qryfcn,rows_months,months_str, cols))
        p.close()
        p.join()

    '''
    for kwargs in getrows(qryfcn,rows_months, months_str, cols):
        explode_month(kwargs, csvfile)

    #os.system(f"sort -t ',' -k {cols['sort_output']} -o raw_{csvfile} {csvfile}.raw.*")
    os.system(f"sort -t ',' -k {cols['sort_output']} --output=output/raw.csv {csvfile}.raw.*")
    os.system(f"sort -t ',' -k {cols['sort_output']} --output=output/filtered.csv {csvfile}.filtered.*")
    #os.system(f'rm {csvfile}.raw*')
    #os.system(f'rm {csvfile}.filtered*')

    print(f'omitted rows (no data in time range): {sum(~exists)}')
    print(f'omitted rows (no mmsi): {len(qryrows) - sum(validmmsi)}')

'''
csvfile = 'output/test.csv'
cols = {'mmsi':3, 'start':-2, 'end':-1, 'sort_output':1, 'keepcols':[0,1,2]}
csvfile='output/test.csv'
test = explode_month(csvfile=csvfile, **next(getrows(qryfcn,rows_months,months_str,cols)))
explode(qryfcn, qryrows, cols, csvfile='output/test.csv')


test = list(getrows(qryfcn,row_months,months_str, cols))
'''

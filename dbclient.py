import os
from multiprocessing import Pool#, set_start_method
import time
from functools import partial
from datetime import datetime, timedelta

import numpy as np
from shapely.geometry import Point, LineString, Polygon
#from shapely.prepared import prep

from gis import compute_knots
from database import dt2monthstr, dbconn
from track_gen import trackgen, segment, filtermask, writecsv

#from numba import vectorize
#os.environ['NUMBAPRO_LIBDEVICE'] = "/usr/local/cuda-10.0/nvvm/libdevice"
#os.environ['NUMBAPRO_NVVM'] = "/usr/local/cuda-10.0/nvvm/lib64/libnvvm.so"
#@vectorize
#@vectorize(['None(dict)'], target='cuda')

aisdb = dbconn()
conn = aisdb.conn

def _geofence_proc(track, zones, csvfile=None, staticcols=['mmsi', 'name', 'type', ], keepcols=['time', 'lon', 'lat', 'cog', 'sog']):
    ''' parallel process function for segmenting and geofencing tracks
        appends columns for bathymetry, shore dist, and hull surface area
    '''
    print(track['mmsi'])
    chunksize=5000
    for rng in segment(track, maxdelta=timedelta(hours=2), minsize=3):
        mask = filtermask(track, rng, filters)
        if (n := sum(mask)) == 0: continue
        for c in range(0, (n // chunksize) + 1, chunksize):
            nc = c + chunksize
            # get subset of zones that intersect with track
            in_zones = { k:v for k,v in zones['geoms'].items() if 
                    LineString(zip(track['lon'][rng][mask][c:nc], track['lat'][rng][mask][c:nc])).intersects(v) }
            # from these zones, get zone for individual points
            zoneID = (([k for k,v in in_zones.items() if v.contains(p)] or [None])[0] 
                    for p in map(Point, zip(track['lon'][rng][mask][c:nc], track['lat'][rng][mask][c:nc])) )
            writecsv(
                    np.vstack((
                        #np.array([track['mmsi'] for _ in range(n)])[c:nc],
                        #np.array([track['name'] for _ in range(n)])[c:nc],
                        #np.array([track['type'] for _ in range(n)])[c:nc],
                        #track['time'][rng][mask][c:nc],
                        #track['lon'][rng][mask][c:nc],
                        #track['lat'][rng][mask][c:nc],
                        #track['cog'][rng][mask][c:nc],
                        #track['sog'][rng][mask][c:nc],
                        *(np.array([track[col] for _ in range(n)])[c:nc] for col in staticcols),
                        *(track[col][rng][mask][c:nc] for col in keepcols),
                        np.append(compute_knots(track, rng), [0])[mask][c:nc],
                        list(zoneID),
                        np.array([zones['domain'] for _ in range(n)])[c:nc],
                        #bathymetry=None,#kadlu.load(src='gebco', var='bathy')
                        np.array([None for _ in range(n)])[c:nc], # bathymetry
                        np.array([None for _ in range(n)])[c:nc], # shore dist
                        np.array([None for _ in range(n)])[c:nc], # approx hull area
                    )).T, 
                    csvfile + f'.{track["mmsi"]}', 
                    mode='a'
                )
    return


def geofence(rows, zones, csvfile):
    """ 
        # set start method in main script
        import os; from multiprocessing import set_start_method
        if os.name == 'posix' and __name__ == '__main__': 
            set_start_method('forkserver')
    """
    t1 = datetime.now()
    with open(csvfile, 'w') as f: f.write('mmsi,vessel_name,vessel_type,time,lon,lat,heading_reported,sog_reported,sog_computed,zone_ID,domain_ID,#bathymetry,shore_dist,hull_surface\n')
    with Pool(processes=min(os.cpu_count(), 32)) as p: 
        p.imap_unordered(partial(_geofence_proc, zones=zones, csvfile=csvfile), trackgen(rows), chunksize=1)
        p.close()
        p.join()
    t2 = datetime.now()
    print(f'processed in {(t2-t1).seconds}s')
    assert os.name == 'posix', 'todo: os-agnostic concatenation'
    os.system(f'cat {csvfile}.* >> {csvfile}')
    time.sleep(5)
    os.system(f'rm {csvfile}.*')


'''
res = next(getrows(qryfcn, rows_months, months_str, cols))
gen = explode_month(*next(getrows(qryfcn,rows_months,months_str), cols, csvfile))

'''
"""
if __name__ == '__main__':
    # sample code
    for track in trackgen(rows):
        for rng in segment(track, maxdelta=timedelta(hours=2), minsize=3):
            mask = filtermask(track, rng)
            sog_haversine = compute_knots(track, rng)
            print(f'{track["mmsi"]}  rng: {rng}  sog vs haversine avg diff: '
                  f'{np.average(np.abs(np.append([0],sog_haversine)[mask] - track["sog"][rng][mask]))}')
"""


def getrows(qryfcn, rows_months, months_str, cols):
    '''
    qrows_month, mstr = rows_months[0], months_str[0]
    '''
    callback = lambda alias, ummsi, **_: f'''{alias}.mmsi in ('{"', '".join(ummsi)}')'''
    for qrows_month, mstr in zip(rows_months, months_str):
        print(f'querying {mstr}...')
        ummsi = np.unique(qrows_month[:,cols['mmsi']])
        qry = qryfcn(mstr, callback, dict(ummsi=ummsi)) + '\nORDER BY mmsi, time'
        cur = conn.cursor()
        cur.execute(qry)
        res = np.array(cur.fetchall())
        yield dict(res=res, qrows_month=qrows_month, cols=cols, mstr=mstr)


def explode_month(kwargs, csvfile, keepraw=True):
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
                    np.array([track['name'] for _ in range(n)]), # name 
                    np.array([track['type'] for _ in range(n)]), # type
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
                        np.array([track['name'] for _ in range(n2)]), # name 
                        np.array([track['type'] for _ in range(n2)]), # type
                        track['time'][rng][mask2],
                        track['lon'][rng][mask2],
                        track['lat'][rng][mask2],
                        track['cog'][rng][mask2],
                        track['sog'][rng][mask2],
                        np.append(compute_knots(track, rng), [0]).astype(np.uint16)[mask2],
                    )).T
                ))
                    
    assert len(out) > 0
    if keepraw: writecsv(raw, csvfile + f'.raw.{mstr}', mode='a',)
    writecsv(out, csvfile + f'.filtered.{mstr}', mode='a',)
    

def explode(qryfcn, qryrows, cols, dateformat='%m/%d/%Y', csvfile='output/test.csv'):
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

    with Pool(processes=3) as p:
        p.imap_unordered(partial(explode_month, csvfile=csvfile), getrows(qryfcn,rows_months,months_str, cols))
        p.close()
        p.join()

    '''
    for kwargs in getrows(qryfcn,rows_months, months_str, cols):
        explode_month(kwargs, csvfile)
    '''

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

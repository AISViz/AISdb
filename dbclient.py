import os
from multiprocessing import Pool#, set_start_method
import pickle
import time
from functools import partial
from datetime import datetime, timedelta

import numpy as np
from shapely.geometry import Point, LineString, Polygon


from gis import compute_knots
from database import dt2monthstr, dbconn, epoch_2_dt
from track_gen import trackgen, segment, filtermask, writecsv
from shore_dist import shore_dist_gfw
from webdata import marinetraffic 
from webdata.marinetraffic import scrape_tonnage
from gebco import Gebco
from wsa import wsa



# returns absolute value of bathymetric depths with topographic heights converted to 0
depth_nonnegative = lambda track, zoneset: np.array([d if d >= 0 else 0 for d in track['depth_metres'][zoneset]])

# returns minutes spent within kilometers range from shore
time_in_shoredist_rng = lambda track, subset, dist0=0.01, dist1=5: (
    sum(map(len, segment(
        {'time': track['time'][subset][[dist0 <= d <= dist1 for d in track['km_from_shore'][subset]]]}, 
        maxdelta=timedelta(minutes=1), 
        minsize=1
    )))
)

# collect vessel track statistics
segmentinfo = lambda track, stacked_arr, src_zone, domain: dict(
        mmsi                                =   track['mmsi'],
        imo                                 =   track['imo'] or '',
        vessel_name                         =   track['vessel_name'] or '',
        vessel_type                         =   track['ship_type_txt'] or '',
        domain                              =   domain,
        src_zone                            =   src_zone,
        rcv_zone                            =   '',
        vessel_length                       =   (track['dim_bow'] + track['dim_stern']) or '',
        hull_submerged_surface_area         =   track['submerged_hull_m^2'] or '',
        first_timestamp                     =   epoch_2_dt(track['time'][0]).strftime('%Y-%m-%d %H:%M:%S UTC'),
        last_timestamp                      =   epoch_2_dt(track['time'][-1]).strftime('%Y-%m-%d %H:%M:%S UTC'),
        year                                =   epoch_2_dt(track['time'][0]).year,
        month                               =   epoch_2_dt(track['time'][0]).month,
        day                                 =   epoch_2_dt(track['time'][0]).day,
        ballast                             =   None,
        confidence                          =   0,
    )

# collect stats about a vessel in context of a zone
zone_stats = lambda track, zoneset: dict(
        min_shore_dist                      =   np.min(track['km_from_shore'][zoneset]), 
        avg_shore_dist                      =   np.average(track['km_from_shore'][zoneset]), 
        max_shore_dist                      =   np.max(track['km_from_shore'][zoneset]), 
        min_depth                           =   np.min(depth_nonnegative(track, zoneset)),
        avg_depth                           =   np.average(depth_nonnegative(track, zoneset)),
        max_depth                           =   np.max(depth_nonnegative(track, zoneset)),
        minutes_within_10m_5km_shoredist    =   time_in_shoredist_rng(track, zoneset, 0.01, 5),
        minutes_within_30m_20km_shoredist   =   time_in_shoredist_rng(track, zoneset, 0.03, 20),
        minutes_within_100m_50km_shoredist  =   time_in_shoredist_rng(track, zoneset, 0.1, 50),
        first_seen_in_zone                  =   epoch_2_dt(track['time'][zoneset][0]).strftime('%Y-%m-%d %H:%M:%S UTC'),
        last_seen_in_zone                   =   epoch_2_dt(track['time'][zoneset][-1]).strftime('%Y-%m-%d %H:%M:%S UTC'),
    )


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


def _mergeprocess(track, zones, dbpath, colnames):
    ''' parallel process function for segmenting and geofencing tracks
        appends columns for bathymetry, shore dist, and hull surface area
    '''

    chunksize=500000
    filters = [
            lambda track, rng: compute_knots(track, rng) < 50,
        ]

    filepath = os.path.join(tmpdir(dbpath), str(track['mmsi']))
    if os.path.isfile(filepath): 
        return
    print(f'{track["mmsi"]}\tcount={len(track["time"])}')

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

    statrows = []

    for rng in segment(track, maxdelta=timedelta(hours=3), minsize=1):

        mask = filtermask(track, rng, filters, True)
    
        n = sum(mask)
        if n == 0: continue
        #for c in range(0, (n // chunksize) + 1, chunksize):
        #nc = c + chunksize
        if n / len(mask) < .65 :
            print(f'WARNING: skipped row {track["mmsi"]} {rng}\tconfidence={n / len(mask)}')
            continue

        subset = np.array(rng)[mask]#[c:nc]
        if len(subset) == 1: continue

        # get subset of zones that intersect with track
        in_zones = { k:v for k,v in zones['geoms'].items() if LineString(zip(track['lon'][subset], track['lat'][subset])).intersects(v) }
        if in_zones == {} : continue

        # from these zones, get zone for individual points
        zoneID = np.array(list(([k for k,v in in_zones.items() if v.contains(p)] or [''])[0] 
            for p in map(Point, zip(track['lon'][subset], track['lat'][subset]))), dtype=object)
        #assert not (np.unique(zoneID)[0] == '' and len(np.unique(zoneID)) == 1)

        # append zone context to track rows
        stacked = np.vstack((
                [track['mmsi'] for _ in subset],
                track['time'][subset],
                *(np.array([track[col] for _ in range(n)]) for col in colnames if col in track['static']),
                *(track[col][subset] for col in colnames if col in track['dynamic']),
                np.append(compute_knots(track, rng), [0])[mask],
                zoneID,
                np.array([zones['domain'] for _ in range(n)]),
            )).T

        #assert len(stacked[0]) > 1
        
        # collect transits between zone boundaries
        zonecrossing = np.append(np.append([0], np.where(stacked[:-1,-2] != stacked[1:,-2])[0] +1), [len(stacked)-1])
        if zonecrossing[-2] == zonecrossing[-1]:
            zonecrossing = zonecrossing[:-1]

        # aggregate zone stats at transit nodes using sliding windows
        for zoneidx, nextzoneidx, thirdzoneidx in zip(zonecrossing[:-2], zonecrossing[1:-1], zonecrossing[2:]):
            zoneset= subset[zoneidx : nextzoneidx]
            nextzoneset = subset[nextzoneidx : thirdzoneidx]
            
            src_zone = zoneID[zoneidx]
            track_stats = segmentinfo(track, stacked, src_zone=src_zone, domain=zones['domain'])
            track_stats['confidence'] = sum(mask) / len(mask)
            track_stats['rcv_zone'] = zoneID[nextzoneidx]
            assert not track_stats['src_zone'] == track_stats['rcv_zone']

            src_zone_stats = zone_stats(track, zoneset)
            rcv_zone_stats = zone_stats(track, nextzoneset)
            track_stats['src_stats'] = src_zone_stats
            track_stats['rcv_stats'] = rcv_zone_stats

            statrows.append(track_stats)

        zoneset = subset[zonecrossing[-2]:]

        src_zone = zoneID[zonecrossing[-2]]
        track_stats =  segmentinfo(track, stacked, src_zone, domain=zones['domain'])

        src_zone_stats = zone_stats(track, zoneset)
        track_stats['src_stats'] = src_zone_stats
        statrows.append(track_stats)

    if len(statrows) == 0: 
        return 

    with open(filepath, 'ab') as f:
        for track_stats in statrows:
            pickle.dump(track_stats, f)

    return 


def tmpdir(dbpath):
    path, dbfile = dbpath.rsplit(os.path.sep, 1)
    tmpdirpath = os.path.join(path, 'tmp_parsing')
    if not os.path.isdir(tmpdirpath):
        os.mkdir(tmpdirpath)
    return tmpdirpath


def merge_layers(rowgen, zones, dbpath):
    """ # set start method in main script
        import os; from multiprocessing import set_start_method
        if os.name == 'posix' and __name__ == '__main__': 
            set_start_method('forkserver')
    """

    # read data layers from disk to merge with AIS
    print('aggregating ais, shore distance, bathymetry, vessel geometry...')
    with shore_dist_gfw(dbpath=dbpath) as sdist, Gebco(dbpath=dbpath) as bathymetry, marinetraffic.scrape_tonnage(dbpath=dbpath) as hullgeom:

        for rows in rowgen:
            #rows = np.array(list(rowgen))

            xy = rows[:,2:4]
            mmsi_column, imo_column, ship_type_column = 0, 7, 13

            # vessel geometry
            #print('aggregating unique mmsi, imo...')
            uniqueID = {}
            _ = [uniqueID.update({f'{r[mmsi_column]}_{r[imo_column]}' : {'m' : r[mmsi_column], 'i' : r[imo_column]}}) for r in rows]

            #print('loading marinetraffic vessel data...')
            for uid in uniqueID.values():
                ummsi, uimo = uid.values()
                if uimo != None:
                    uid['dwt'] = hullgeom.get_tonnage_mmsi_imo(ummsi, uimo)
                else:
                    uid['dwt'] = 0

            deadweight_tonnage = np.array([uniqueID[f'{r[mmsi_column]}_{r[imo_column]}']['dwt'] for r in rows ])

            # wetted surface area - regression on tonnage and ship type
            ship_type = np.logical_or(rows[:,ship_type_column], [0 for _ in range(len(rows))])
            submerged_hull = np.array([wsa(d, r) for d,r in zip(deadweight_tonnage,ship_type) ])

            # shore distance from cell grid
            #print('loading shore distance...')
            km_from_shore = np.array([sdist.getdist(x, y) for x, y in xy ])

            # seafloor depth from cell grid
            #print('loading bathymetry...')
            depth = np.array([bathymetry.getdepth(x, y) for x,y in xy ]) * -1

            yield np.hstack((rows, np.vstack((deadweight_tonnage, submerged_hull, km_from_shore, depth)).T))
        #print('merging...')
        #merged = np.hstack((rows, np.vstack((deadweight_tonnage, submerged_hull, km_from_shore, depth)).T))

    #return merged


def concat_layers(merged, zones, dbpath):
    #merged = merge_layers(rows, zones, dbpath)

    colnames = [
        'mmsi', 'time', 'lon', 'lat', 
        'cog', 'sog', 'msgtype',
        'imo', 'vessel_name',  
        'dim_bow', 'dim_stern', 'dim_port', 'dim_star',
        'ship_type', 'ship_type_txt',
        'deadweight_tonnage', 'submerged_hull_m^2',
        'km_from_shore', 'depth_metres',
    ]

    print('aggregating...')
    for track in merged:
        #_mergeprocess(track, zones, dbpath, colnames)
        _mergeprocess(next(trackgen(track, colnames=colnames)), zones, dbpath, colnames)

    '''
    with Pool(processes=4) as p:
        # define fcn as _mergeprocess() with zones, db context as static args
        fcn = partial(_mergeprocess, zones=zones, dbpath=dbpath, colnames=colnames)
        # map track generator to anonymous fcn for each process in processing pool
        p.imap_unordered(fcn, (next(trackgen(m, colnames=colnames)) for m in merged), chunksize=1)
        p.close()
        p.join()
    '''
    _ = [colnames.append(col) for col in ['sog_computed', 'zone', 'domain']]

    picklefiles = [fname for fname in sorted(os.listdir(tmpdir(dbpath))) if '_' not in fname]

    rowfromdict = lambda d: ','.join(map(str, [val if not type(val) == dict else ','.join(map(str, val.values())) for val in d.values()]))

    header = lambda d: (','.join([
        ','.join([k for k,v in d.items() if type(v) != dict ]) , 
        ','.join(['src_' + s for s in map(str, [val for val in d['src_stats'].keys() ])]) , 
        ','.join(['rcv_' + s for s in map(str, [val for val in d['src_stats'].keys() ])]),
        ]) + '\n')

    dirpath, dbfile = dbpath.rsplit(os.path.sep, 1)
    csvfile = dirpath + os.path.sep + 'output.csv'

    with open(os.path.join(tmpdir(dbpath), picklefiles[0]), 'rb') as f0, open(csvfile, 'w') as f1:
        f1.write(header(pickle.load(f0)))
    with open(csvfile, 'a') as output:
        for picklefile in picklefiles:
            #results = np.ndarray(shape=(0, len(colnames)) )
            results = []
            with open(os.path.join(tmpdir(dbpath), picklefile), 'rb') as f:
                while True:
                    try:
                        getrow = pickle.load(f)
                    except EOFError as e:
                        break
                    except Exception as e:
                        raise e
                    #results = np.vstack((results, getrows))
                    results.append(rowfromdict(getrow))
            output.write('\n'.join(results) +'\n')
            os.remove(os.path.join(tmpdir(dbpath), picklefile))

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

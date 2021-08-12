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
        #confidence                          =   0,
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
        
        yields merged sets of rows with regions context 
    '''

    chunksize=500000
    filters = [
            lambda track, rng: compute_knots(track, rng) < 50,
        ]

    filepath = os.path.join(tmpdir(dbpath), str(track['mmsi']).zfill(9))
    if os.path.isfile(filepath): 
        print(f'skipping {track["mmsi"]}')
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
        #print(rng)

        mask = filtermask(track, rng, filters, True)
    
        n = sum(mask)
        if n == 0: continue
        #for c in range(0, (n // chunksize) + 1, chunksize):
        #nc = c + chunksize
        #if n / len(mask) < .65 :
        #    print(f'WARNING: skipped row {track["mmsi"]} {rng}\tconfidence={n / len(mask)}')
        #    continue

        subset = np.array(rng)[mask]#[c:nc]
        if len(subset) == 1: continue

        # get subset of zones that intersect with track
        in_zones = {}
        for zonerng in range(0, len(subset), 1000):
            if not len(track['lon'][subset][zonerng:zonerng+1000]) > 1:
                print('warning: skipping track segment of length 1')
                continue
            in_zones.update({ k:v for k,v in zones['geoms'].items() if LineString(zip(track['lon'][subset][zonerng:zonerng+1000], track['lat'][subset][zonerng:zonerng+1000])).intersects(v) })

        #in_zones = { k:v for k,v in zones['geoms'].items() if LineString(zip(track['lon'][subset], track['lat'][subset])).intersects(v) }
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
            #track_stats['confidence'] = sum(mask) / len(mask)
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
    ''' generator function to merge AIS row data with shore distance, bathymetry, and geometry databases

        args:
            rowgen: generator function 
                yields sets of rows grouped by MMSI sorted by time
            zones: dictionary
                see zones_from_txts() function in gis.py
                returns geometry objects as dictionary values
            dbpath: string
                path to .db file

        yields:
            sets of rows grouped by MMSI sorted by time with additional columns appended

    """ # set start method in main script
        import os; from multiprocessing import set_start_method
        if os.name == 'posix' and __name__ == '__main__': 
            set_start_method('forkserver')
    """

    '''

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


def concat_layers(merged, zones, dbpath, parallel=False):
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

    if not parallel: 
        for mmsiset in merged:
            _mergeprocess(next(trackgen(mmsiset, colnames=colnames)), zones, dbpath, colnames)
    else:
        with Pool(processes=12) as p:
            # define fcn as _mergeprocess() with zones, db context as static args
            fcn = partial(_mergeprocess, zones=zones, dbpath=dbpath, colnames=colnames)
            # map track generator to anonymous fcn for each process in processing pool
            p.imap_unordered(fcn, (next(trackgen(m, colnames=colnames)) for m in merged), chunksize=1)
            p.close()
            p.join()

    _ = [colnames.append(col) for col in ['sog_computed', 'zone', 'domain']]

    picklefiles = [fname for fname in sorted(os.listdir(tmpdir(dbpath))) if '_' not in fname]

    rowfromdict = lambda d: ','.join(map(str, [val if not type(val) == dict else ','.join(map(str, val.values())) for val in d.values()]))

    header = lambda d: (','.join([
        ','.join([k for k,v in d.items() if type(v) != dict ]) , 
        ','.join(['src_' + s for s in map(str, [val for val in d['src_stats'].keys() ])]), 
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



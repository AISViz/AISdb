import os
from multiprocessing import Pool#, set_start_method
import pickle
import time
from functools import partial, reduce
from datetime import datetime, timedelta

import numpy as np
from shapely.geometry import Point, LineString, Polygon

from common import *
from gis import delta_knots, delta_meters, delta_seconds, ZoneGeom, Domain
from database import dt2monthstr, dbconn, epoch_2_dt
from track_gen import trackgen, segment, filtermask, writecsv
from merge_data import merge_layers


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
segmentinfo = lambda track_static, stacked_arr, src_zone, domainname: dict(
        mmsi                                =   track_static['mmsi'],
        imo                                 =   track_static['imo'] or '',
        vessel_name                         =   track_static['vessel_name'] or '',
        vessel_type                         =   track_static['ship_type_txt'] or '',
        domainname                          =   domainname,
        src_zone                            =   src_zone,
        rcv_zone                            =   'NULL',
        transit_nodes                       =   'NULL',
        vessel_length                       =   (track_static['dim_bow'] + track_static['dim_stern']) or '',
        hull_submerged_surface_area         =   track_static['submerged_hull_m^2'] or '',
        ballast                             =   None,
    )


# collect stats about a vessel in context of a zone
zone_stats = lambda track, zoneset: dict(
        first_seen_in_zone                  =   epoch_2_dt(track['time'][zoneset][0]).strftime('%Y-%m-%d %H:%M UTC'),
        last_seen_in_zone                   =   epoch_2_dt(track['time'][zoneset][-1]).strftime('%Y-%m-%d %H:%M UTC'),
        year                                =   epoch_2_dt(track['time'][zoneset][0]).year,
        month                               =   epoch_2_dt(track['time'][zoneset][0]).month,
        day                                 =   epoch_2_dt(track['time'][zoneset][0]).day,
        total_distance_meters               =   np.sum(delta_meters(track, zoneset[[0,-1]])),
        cumulative_distance_meters          =   np.sum(delta_meters(track, zoneset)),
        min_shore_dist                      =   np.min(track['km_from_shore'][zoneset]), 
        avg_shore_dist                      =   np.average(track['km_from_shore'][zoneset]), 
        max_shore_dist                      =   np.max(track['km_from_shore'][zoneset]), 
        min_depth                           =   np.min(depth_nonnegative(track, zoneset)),
        avg_depth                           =   np.average(depth_nonnegative(track, zoneset)),
        max_depth                           =   np.max(depth_nonnegative(track, zoneset)),
        velocity_knots_min                  =   np.min(delta_knots(track, zoneset)),
        velocity_knots_avg                  =   np.average(delta_knots(track, zoneset)),
        velocity_knots_max                  =   np.max(delta_knots(track, zoneset)),
        minutes_spent_in_zone               =   int((epoch_2_dt(track['time'][zoneset][-1]) - epoch_2_dt(track['time'][zoneset][0])).total_seconds()) / 60,
        minutes_within_10m_5km_shoredist    =   time_in_shoredist_rng(track, zoneset, 0.01, 5),
        minutes_within_30m_20km_shoredist   =   time_in_shoredist_rng(track, zoneset, 0.03, 20),
        minutes_within_100m_50km_shoredist  =   time_in_shoredist_rng(track, zoneset, 0.1, 50),
    )





def geofence(track_merged, domain, colnames, filters, maxdelta=timedelta(hours=1), **kwargs):
    ''' collect zone transits using geofenced zone context '''

    def agg_transits_per_segment(track_merged, filters):
        ''' generator function yielding track segments with zone context appended '''

        transits = []
        transit_nodes = []
        transit_edges = []

        for rng in segment(track_merged, maxdelta=maxdelta, minsize=1):
            '''
            if apply_filter:
                filters = [
                        lambda track, rng: delta_knots(track, rng) < 50,
                        lambda track, rng: delta_meters(track, rng) < 10000,
                        lambda track, rng: delta_seconds(track, rng) > 0,
                    ]
                mask = filtermask(track_merged, rng, filters, first_val=True)
                subset = np.array(rng)[mask] 
            
            else:
                subset = np.array(rng)
            '''
            mask = filtermask(track_merged, rng=rng, filters=filters, first_val=True)
            subset = np.array(rng)[mask] 

            zoneID = np.array([domain.point_in_polygon(x, y) for x, y in zip(track_merged['lon'][subset], track_merged['lat'][subset])], dtype=object)
            in_zones = set(zoneID)

            # append zone context to track rows
            transit_window = np.vstack((
                    [track_merged['mmsi'] for _ in subset],
                    track_merged['time'][subset],
                    #*(np.array([track_merged[col] for _ in range(n)]) for col in colnames if col in track_merged['static']),
                    *np.array(list(np.array([track_merged[col] for col in colnames if col in track_merged['static']]) for _ in subset)).T,
                    *(track_merged[col][subset] for col in colnames if col in track_merged['dynamic']),
                    np.append(delta_knots(track_merged, subset), [0]),
                    zoneID,
                    np.array([domain.name for _ in subset]),
                )).T

            transits.append(transit_window)
            transit_nodes.append(in_zones)
            transit_edges.append(zoneID)

        # if vessel never entered any zones, skip it
        if set(['Z0', 'NULL']).issuperset(reduce(set.union, transit_nodes)):
            transits, transit_nodes, transit_edges = [], [], []
        
        # display error message for erroneous tracks
        for i in range(len(transits)-1, -1, -1):
            if len(transits[i]) == 1:
                print(f'''skipping track segment with single message within a {
                        int(maxdelta.total_seconds()*2//60)
                    }-minute period\tmmsi: {
                        transits[i][0][0] 
                    }\ttime: {
                        epoch_2_dt(transits[i][0][1]).strftime('%Y-%m-%d %H:%M')
                    }\tzone: {
                        transits[i][0]
                    }''')
                _,_,_ = transits.pop(i), transit_nodes.pop(i), transit_edges.pop(i)
                continue

            assert transits[i][:,1][-1] != transits[i][:,1][0]

            elapsed_minutes = transits[i][:,1][-1] - transits[i][:,1][0]
            num_transits = sum(transit_edges[i][:-1] != transit_edges[i][1:])
            zones_per_deltatime = num_transits / (elapsed_minutes / (maxdelta.total_seconds() / 60))

            if zones_per_deltatime > 5:
                print(f'''invalid track! duplicate mmsis?    mmsi:{
                        track_merged["mmsi"]
                    }    time range: {
                        epoch_2_dt(transits[i][:,1][0]).strftime('%Y-%m-%d %H:%M')
                    } UTC .. {
                        epoch_2_dt(transits[i][:,1][-1]).strftime('%Y-%m-%d %H:%M')
                    } UTC    avg zones per {
                        int(maxdelta.total_seconds()//60)
                    } minutes: {
                        zones_per_deltatime:.2f}''')

                _,_,_ = transits.pop(i), transit_nodes.pop(i), transit_edges.pop(i)

        # concatenate non-transiting segments, yield zone activity
        while len(transits) > 0:
            if  (len(transits) > 1 
                    and len(transit_nodes[0]) == 1 == len(transit_nodes[1]) 
                    and transit_nodes[0] == transit_nodes[1]):
                transits[0] = np.vstack((transits[0], transits.pop(1)))
                transit_nodes[0] = transit_nodes[0].union(transit_nodes.pop(1))
                transit_edges[0] = np.append(transit_edges[0], transit_edges.pop(1))
            else:
                yield transits.pop(0), transit_nodes.pop(0), transit_edges.pop(0)

    ### END NESTED FUNCTION DEFINITION ###


    track_static = {k:track_merged[k] for k in track_merged['static'].union(set(['mmsi']))}

    # collect transits between zone boundaries
    statrows = []
    for transit_window, in_zones, zoneID in agg_transits_per_segment(track_merged, filters):
        # ignore transits where only a single message was seen in the zone (zero-length zone events)
        intersection = np.append(np.where(transit_window[:-1,-2] != transit_window[1:,-2])[0] +1, [len(transit_window)-1])
        transit_mask = intersection[np.append(intersection[:-1] == intersection[1:] - 1, [False])]
        while len(transit_mask) > 0:
            print(f'''warning: can't vectorize track segments of length 1. ignoring msgs for mmsi {
                track_static['mmsi']:9d} with timestamps in zones:{
                    list(zip([d.strftime('%Y-%m-%d %H:%M UTC') for d in epoch_2_dt(transit_window[:,1][transit_mask])], zoneID[transit_mask]))
                }''')
            #intersection_prefilter = np.append(np.append([0], np.where(transit_window[:-1,-2] != transit_window[1:,-2])[0] +1), [len(transit_window)-1])
            setrng = np.array(list(set(np.array(range(len(transit_window)))) - set(transit_mask)))
            transit_window = transit_window[setrng]
            zoneID = zoneID[setrng]
            intersection = np.append(np.append([0], np.where(transit_window[:-1,-2] != transit_window[1:,-2])[0] +1), [len(transit_window)-1])
            transit_mask = intersection[np.append(intersection[:-1] == intersection[1:] - 1, [False])]
        #else:
        #    intersection = intersection_prefilter

        setrng = np.array(range(len(transit_window)))

        intersection = np.append(np.append([0], intersection), [len(setrng)-1])
        intersection = np.array(sorted(list(set(intersection))))
        #if intersection[-2] == intersection[-1]: intersection = intersection[:-1]

        if len(intersection) > 2: 
            #intersection = np.append([0], intersection)
            # aggregate zone stats at transit nodes using sliding windows
            for zoneidx, nextzoneidx, thirdzoneidx in zip(intersection[:-2], intersection[1:-1], intersection[2:]):
                zoneset = setrng[zoneidx : nextzoneidx]
                if len(zoneset) == 1:
                    #zoneset = np.append(zoneset, setrng[zoneidx: nextzoneidx])
                    print(f'''warning: skipping transit for mmsi {
                            track_static['mmsi']
                        } with only a single observation in zone {
                            zoneID[zoneidx]
                        }''')
                    continue
                nextzoneset = setrng[nextzoneidx : thirdzoneidx]
                #if len(nextzoneset) == 1: 
                #    nextzoneset = np.append(nextzoneset, setrng[nextzoneidx : thirdzoneidx])
                
                src_zone = zoneID[zoneidx]
                track_stats = segmentinfo(track_static, transit_window, src_zone=src_zone, domainname=domain.name)
                track_stats['src_stats'] = zone_stats(track_merged, zoneset)
                track_stats['rcv_zone']  = zoneID[nextzoneidx]
                track_stats['rcv_stats'] = zone_stats(track_merged, nextzoneset)
                track_stats['transit_nodes'] = f"{track_stats['src_zone']}_{track_stats['rcv_zone']}"
                statrows.append(track_stats)
        else:
            if in_zones == {'Z0'}: continue 
            zoneset = setrng[intersection[-2]:]
            track_stats = segmentinfo(track_static, transit_window, src_zone=zoneID[intersection[-2]], domainname=domain.name)
            track_stats['src_stats'] = zone_stats(track_merged, zoneset)
            track_stats['transit_nodes'] = f"{track_stats['src_zone']}"
            statrows.append(track_stats)

    if len(statrows) == 0: 
        return 

    filepath = os.path.join(tmp_dir, str(track_merged['mmsi']).zfill(9))
    with open(filepath, 'ab') as f:
        for track_stats in statrows:
            pickle.dump(track_stats, f)

    return 


def graph(merged, domain, parallel=0,            
            filters=[
                lambda track, rng: [True for _ in rng[:-1]],
                ]
            ,
            **kwargs):

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
            geofence(next(trackgen(mmsiset, colnames=colnames)), domain=domain, colnames=colnames, filters=filters, **kwargs)
    else:
        with Pool(processes=parallel) as p:
            fcn = partial(geofence, domain=domain, colnames=colnames, filters=filters, **kwargs)
            p.imap_unordered(fcn, (next(trackgen(m, colnames=colnames)) for m in merged), chunksize=1)
            p.close()
            p.join()

    _ = [colnames.append(col) for col in ['sog_computed', 'zone', 'domain']]

    picklefiles = [fname for fname in sorted(os.listdir(tmp_dir)) if '_' not in fname]

    rowfromdict = lambda d: ','.join(map(str, [val if not type(val) == dict else ','.join(map(str, val.values())) for val in d.values()]))

    header = lambda d: (','.join([
        ','.join([k for k,v in d.items() if type(v) != dict ]),
        ','.join(['src_' + s for s in map(str, [val for val in d['src_stats'].keys() ])]),
        ','.join(['rcv_' + s for s in map(str, [val for val in d['src_stats'].keys() ])]),
        ]) + '\n')

    csvfile = os.path.join(data_dir, 'output.csv')

    with open(os.path.join(tmp_dir, picklefiles[0]), 'rb') as f0, open(csvfile, 'w') as f1:
        f1.write(header(pickle.load(f0)))
    with open(csvfile, 'a') as output:
        for picklefile in picklefiles:
            #results = np.ndarray(shape=(0, len(colnames)) )
            results = []
            with open(os.path.join(tmp_dir, picklefile), 'rb') as f:
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
            os.remove(os.path.join(tmp_dir, picklefile))

    return 



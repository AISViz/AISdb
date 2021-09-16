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


# categorical vessel data
staticinfo = lambda track, domain: dict(
        mmsi                                =   track['mmsi'],
        imo                                 =   track['imo'] or '',
        vessel_name                         =   track['vessel_name'] or '',
        vessel_type                         =   track['ship_type_txt'] or '',
        domainname                          =   domain.name,
        vessel_length                       =   (track['dim_bow'] + track['dim_stern']) or '',
        hull_submerged_surface_area         =   track['submerged_hull_m^2'] or '',
        #ballast                             =   None,
    )


# collect aggregated statistics on vessel positional data 
transitinfo = lambda track, zoneset: dict(
        src_zone                            =   track['in_zone'][zoneset][0],
        rcv_zone                            =   track['in_zone'][zoneset][-1],
        transit_nodes                       =   f"{track['in_zone'][zoneset][0]}_{track['in_zone'][zoneset][-1]}",
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
        velocity_knots_min                  =   np.min(delta_knots(track, zoneset)) if len(zoneset) > 1 else 'NULL',
        velocity_knots_avg                  =   np.average(delta_knots(track, zoneset)) if len(zoneset) > 1 else 'NULL',
        velocity_knots_max                  =   np.max(delta_knots(track, zoneset)) if len(zoneset) > 1 else 'NULL',
        minutes_spent_in_zone               =   int((epoch_2_dt(track['time'][zoneset][-1]) - epoch_2_dt(track['time'][zoneset][0])).total_seconds()) / 60 if len(zoneset) > 1 else 'NULL',
        minutes_within_10m_5km_shoredist    =   time_in_shoredist_rng(track, zoneset, 0.01, 5),
        minutes_within_30m_20km_shoredist   =   time_in_shoredist_rng(track, zoneset, 0.03, 20),
        minutes_within_100m_50km_shoredist  =   time_in_shoredist_rng(track, zoneset, 0.1, 50),
    )


def geofence(track_merged, domain):
    ''' compute points-in-polygons for every positional report in a vessel 
        trajectory. at each track position where the zone changes, a transit 
        index is recorded, and trajectory statistics are aggregated for this
        index range using staticinfo() and transitinfo()
        
        results will be serialized as binary files labelled by mmsi into the 
        'tmp_dir' directory, as defined in the config file. see graph() for
        deserialization and concatenation of results
        
        args:
            track_merged: dict
                dictionary of vessel trajectory data, as output by 
                ais.track_gen.trackgen() or its wrapper functions
            domain: ais.gis.Domain() class object
                collection of zones defined as polygons, these will
                be used as nodes in the network graph

        returns: None
    '''

    track_merged['in_zone'] = np.array([domain.point_in_polygon(x, y) for x, y in zip(track_merged['lon'], track_merged['lat'])], dtype=object)
    transits = np.where(track_merged['in_zone'][:-1] != track_merged['in_zone'][1:])[0] +1

    filepath = os.path.join(tmp_dir, str(track_merged['mmsi']).zfill(9))
    with open(filepath, 'ab') as f:

        if len(transits) == 0: 
            rng = np.array(range(len(track_merged['in_zone'])))
            track_stats = staticinfo(track_merged, domain)
            track_stats.update(transitinfo(track_merged, rng))
            track_stats['rcv_zone'] = 'NULL'
            track_stats['transit_nodes'] = track_stats['src_zone']
            pickle.dump(track_stats, f)
            return 

        for i in range(len(transits)-1):
            rng = np.array(range(transits[i], transits[i+1]+1))
            track_stats = staticinfo(track_merged, domain)
            track_stats.update(transitinfo(track_merged, rng))
            pickle.dump(track_stats, f)
    return


def graph(merged, domain, parallel=0):
    ''' perform geofencing on vessel trajectories, then concatenate aggregated 
        transit statistics between nodes (zones) to create network edges from 
        vessel trajectories

        this function will call geofence() for each trajectory in parallel, 
        outputting serialized results to the tmp_dir directory. after 
        deserialization, the temporary files are removed, and output will be 
        written to 'output.csv' inside the data_dir directory

        args:
            merged: ais.track_gen.trackgen() trajectory iterator (or one of its wrapper functions)
                intended to be used with the ais.merge_data.merge_layers() 
                wrapper, but should work with any of the wrappers
            domain: ais.gis.Domain() class object
                collection of zones defined as polygons, these will
                be used as nodes in the network graph
            parallel: integer
                number of processes to compute geofencing in parallel.
                if set to 0 or False, no parallelization will be used

        returns: None
    '''
    
    if not parallel: 
        for track_merged in merged:
            geofence(track_merged, domain=domain)
    else:
        with Pool(processes=parallel) as p:
            fcn = partial(geofence, domain=domain)
            p.imap_unordered(fcn, merged, chunksize=1)
            p.close()
            p.join()

    picklefiles = [os.path.join(tmp_dir, fname) for fname in sorted(os.listdir(tmp_dir)) if '_' not in fname]
    outputfile = os.path.join(data_dir, 'output.csv')

    with open(outputfile, 'a') as output:

        with open(picklefiles[0], 'rb') as f0:
            getrow = pickle.load(f0)
            output.write(','.join(map(str, getrow.keys())) + '\n')

        for picklefile in picklefiles:
            results = []
            with open(picklefile, 'rb') as f:
                while True:
                    try:
                        getrow = pickle.load(f)
                    except EOFError as e:
                        break
                    except Exception as e:
                        raise e
                    results.append(','.join(map(str, getrow.values())))
            output.write('\n'.join(results) +'\n')
            os.remove(os.path.join(tmp_dir, picklefile))


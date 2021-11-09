import os
from multiprocessing import Pool#, set_start_method
import pickle
from functools import partial, reduce
from datetime import datetime, timedelta

import numpy as np
from shapely.geometry import Point, LineString, Polygon

from common import *
from gis import delta_knots, delta_meters, delta_seconds, ZoneGeom, Domain
from database import dt2monthstr, dbconn, epoch_2_dt
from track_gen import trackgen, segment, segment_tracks_timesplits
from clustering import segment_tracks_dbscan


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
        cluster_label                       =   track['cluster_label'] if 'cluster_label' in track.keys() else '',
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
        total_distance_meters               =   np.sum(delta_meters(track, zoneset[[0,-1]])).astype(int),
        cumulative_distance_meters          =   np.sum(delta_meters(track, zoneset)).astype(int),
        min_shore_dist                      =   f"{np.min(track['km_from_shore'][zoneset]):.2f}", 
        avg_shore_dist                      =   f"{np.average(track['km_from_shore'][zoneset]):.2f}", 
        max_shore_dist                      =   f"{np.max(track['km_from_shore'][zoneset]):.2f}", 
        min_depth                           =   f"{np.min(depth_nonnegative(track, zoneset)):.2f}",
        avg_depth                           =   f"{np.average(depth_nonnegative(track, zoneset)):.2f}",
        max_depth                           =   f"{np.max(depth_nonnegative(track, zoneset)):.2f}",
        avg_avg_depth_border_cells          =   f"{np.average(track['depth_border_cells_average'])}",
        velocity_knots_min                  =   f"{np.min(delta_knots(track, zoneset)):.2f}" if len(zoneset) > 1 else 'NULL',
        velocity_knots_avg                  =   f"{np.average(delta_knots(track, zoneset)):.2f}" if len(zoneset) > 1 else 'NULL',
        velocity_knots_max                  =   f"{np.max(delta_knots(track, zoneset)):.2f}" if len(zoneset) > 1 else 'NULL',
        minutes_spent_in_zone               =   int((epoch_2_dt(track['time'][zoneset][-1]) - epoch_2_dt(track['time'][zoneset][0])).total_seconds()) / 60 if len(zoneset) > 1 else 'NULL',
        minutes_within_10m_5km_shoredist    =   time_in_shoredist_rng(track, zoneset, 0.01, 5),
        minutes_within_30m_20km_shoredist   =   time_in_shoredist_rng(track, zoneset, 0.03, 20),
        minutes_within_100m_50km_shoredist  =   time_in_shoredist_rng(track, zoneset, 0.1, 50),
    )


fence = lambda track, domain: np.array([domain.point_in_polygon(x, y) for x, y in zip(track['lon'], track['lat'])], dtype=object)


def concat_tracks_no_movement(tracks, domain):

    concatenated = next(tracks)
    concatenated['in_zone'] = fence(concatenated, domain)

    for track in tracks:
        track['in_zone'] = fence(track, domain)
        if (        concatenated['mmsi']                == track['mmsi'] 
                and concatenated['cluster_label']       == track['cluster_label']
                and len(set(concatenated['in_zone']))   == 1 
                and set(concatenated['in_zone'])        == set(track['in_zone'])):
            concatenated = dict(
                    static = concatenated['static'],
                    dynamic = set(concatenated['dynamic']).union(set(['in_zone'])),
                    in_zone = np.append(concatenated['in_zone'], track['in_zone']),
                    **{k:track[k] for k in concatenated['static']},
                    **{k:np.append(concatenated[k], track[k]) for k in concatenated['dynamic'] if k != 'in_zone'},
                )
        else:
            yield concatenated
            concatenated = track

    yield concatenated



def geofence(merged_set, domain, max_cluster_dist_km=50, maxdelta=timedelta(hours=2)):
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

    timesplit = partial(segment_tracks_timesplits,  maxdelta=maxdelta)
    distsplit = partial(segment_tracks_dbscan,      max_cluster_dist_km=max_cluster_dist_km)
    concat    = partial(concat_tracks_no_movement,  domain=domain)

    for track in concat(distsplit(timesplit([merged_set]))):

        filepath = os.path.join(tmp_dir, str(track['mmsi']).zfill(9))

        with open(filepath, 'ab') as f:
            transits = np.where(track['in_zone'][:-1] != track['in_zone'][1:])[0] +1

            for i in range(len(transits)-1):
                rng = np.array(range(transits[i], transits[i+1]+1))
                track_stats = staticinfo(track, domain)
                track_stats.update(transitinfo(track, rng))
                pickle.dump(track_stats, f)

            i0 = transits[-1] if len(transits) >= 1 else 0
            rng = np.array(range(i0, len(track['in_zone'])))
            track_stats = staticinfo(track, domain)
            track_stats.update(transitinfo(track, rng))
            track_stats['rcv_zone'] = 'NULL'
            track_stats['transit_nodes'] = track_stats['src_zone']
            pickle.dump(track_stats, f)

    return


def graph(merged, domain, parallel=0, filters=[lambda rowdict: False]):
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
            filters: list of callables
                each callable function should accept a dictionary describing a 
                network edge as input. if any of the callables return True, 
                the edge will be filtered from the output rows. see staticinfo()
                and transitinfo() above for more info on network edge dict keys
                
                for example, to filter all rows where the max speed exceeds 50 
                knots, and filter non-transiting vessels from zone Z0:

                >>> filters = [
                    lambda rowdict: rowdict['velocity_knots_max'] == 'NULL' or float(rowdict['velocity_knots_max']) > 50,
                    lambda rowdict: rowdict['src_zone'] == 'Z0' and rowdict['rcv_zone'] == 'NULL'
                ]
                
        returns: None
    '''
    
    if not parallel: 
        for track_merged in merged:
            geofence(track_merged, domain=domain)
    else:
        with Pool(processes=parallel) as p:
            fcn = partial(geofence, domain=domain)
            #p.map(fcn, (list(m) for m in merged), chunksize=1)  # better tracebacks for debug
            p.imap_unordered(fcn, merged, chunksize=1)
            p.close()
            p.join()

    outputfile = os.path.join(output_dir, 'output.csv')
    picklefiles = [os.path.join(tmp_dir, fname) for fname in sorted(os.listdir(tmp_dir)) if '_' not in fname]
    assert len(picklefiles) > 0, 'failed to geofence any data... try running again with parallel=0'

    with open(outputfile, 'w') as output:

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
                    if not reduce(np.logical_or, [f(getrow) for f in filters]):
                        results.append(','.join(map(str, getrow.values())))
            os.remove(picklefile)
            if len(results) == 0: continue
            output.write('\n'.join(results) + '\n')


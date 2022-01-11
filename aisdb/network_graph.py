''' collect vessel transits between zones (nodes), and aggregate various trajectory statistics '''

import os
from multiprocessing import Pool
import pickle
from functools import partial, reduce
from datetime import timedelta

import numpy as np

from aisdb import tmp_dir, output_dir
from gis import (
    delta_knots,
    delta_meters,
    epoch_2_dt,
)
from track_gen import (
    fence_tracks,
    segment_rng,
    segment_tracks_encode_greatcircledistance,
    trackgen,
    # max_tracklength,
    # segment_tracks_timesplits,
)
# from webdata.merge_data import merge_layers

# returns absolute value of bathymetric depths with topographic heights converted to 0
depth_nonnegative = lambda track, zoneset: np.array(
    [d if d >= 0 else 0 for d in track['depth_metres'][zoneset]])

# returns minutes spent within kilometers range from shore
time_in_shoredist_rng = lambda track, subset, dist0=0.01, dist1=5: (sum(
    map(
        len,
        segment_rng(
            {
                'time':
                track['time'][subset]
                [[dist0 <= d <= dist1 for d in track['km_from_shore'][subset]]]
            },
            maxdelta=timedelta(minutes=1),
            minsize=1))))

# categorical vessel data
#staticinfo = lambda track, domain: dict(
staticinfo = lambda track: dict(
    mmsi=track['mmsi'],
    imo=track['imo'] or '',
    label=track['label'] if 'label' in track.keys() else '',
    vessel_name=str(track['vessel_name']).replace("'", '').replace('"', '').
    replace(',', '').replace('`', '') or '',
    #vessel_type=track['ship_type_txt'] or '',
    #domainname                          =   domain.name,
    vessel_length=(track['dim_bow'] + track['dim_stern']) or '',
    hull_submerged_surface_area=track['submerged_hull_m^2']
    if 'submerged_hull_m^2' in track.keys() else '',
    #ballast                             =   None,
)

# collect aggregated statistics on vessel positional data
transitinfo = lambda track, zoneset: dict(
    src_zone=f"{int(track['in_zone'][zoneset][0].split('Z')[1]):03}",
    rcv_zone=f"{int(track['in_zone'][zoneset][-1].split('Z')[1]):03}",
    transit_nodes=
    f"{track['in_zone'][zoneset][0]}_{track['in_zone'][zoneset][-1]}",
    num_datapoints=len(track['time'][zoneset]),
    first_seen_in_zone=epoch_2_dt(track['time'][zoneset][0]).strftime(
        '%Y-%m-%d %H:%M UTC'),
    last_seen_in_zone=epoch_2_dt(track['time'][zoneset][-1]).strftime(
        '%Y-%m-%d %H:%M UTC'),
    year=epoch_2_dt(track['time'][zoneset][0]).year,
    month=epoch_2_dt(track['time'][zoneset][0]).month,
    day=epoch_2_dt(track['time'][zoneset][0]).day,
    total_distance_meters=np.sum(delta_meters(track, zoneset[[0, -1]])).astype(
        int),
    cumulative_distance_meters=np.sum(delta_meters(track, zoneset)).astype(int
                                                                           ),
    #min_shore_dist=f"{np.min(track['km_from_shore'][zoneset]):.2f or None}",
    #avg_shore_dist=
    #f"{np.average(track['km_from_shore'][zoneset]):.2f if 'km_from_shore' in track.keys() else None}",
    #max_shore_dist=
    #f"{np.max(track['km_from_shore'][zoneset]):.2f if 'km_from_shore' in track.keys() else None}",
    #min_depth=
    #f"{np.min(depth_nonnegative(track, zoneset)):.2f if 'depth_metres' in track.keys() else None}",
    #avg_depth=
    #f"{np.average(depth_nonnegative(track, zoneset)):.2f if 'depth_metres' in track.keys() else None}",
    #max_depth=
    #f"{np.max(depth_nonnegative(track, zoneset)):.2f if 'depth_metres' in track.keys() else None}",
    #avg_avg_depth_border_cells=
    #f"{np.average(track['depth_border_cells_average'][zoneset]) if 'depth_metres' in track.keys() else None}",
    velocity_knots_min=f"{np.min(delta_knots(track, zoneset)):.2f}"
    if len(zoneset) > 1 else 'NULL',
    velocity_knots_avg=f"{np.average(delta_knots(track, zoneset)):.2f}"
    if len(zoneset) > 1 else 'NULL',
    velocity_knots_max=f"{np.max(delta_knots(track, zoneset)):.2f}"
    if len(zoneset) > 1 else 'NULL',
    minutes_spent_in_zone=int((epoch_2_dt(track['time'][zoneset][
        -1]) - epoch_2_dt(track['time'][zoneset][0])).total_seconds()) / 60
    if len(zoneset) > 1 else 'NULL',
    #minutes_within_10m_5km_shoredist=time_in_shoredist_rng(
    #    track, zoneset, 0.01, 5),
    #minutes_within_30m_20km_shoredist=time_in_shoredist_rng(
    #    track, zoneset, 0.03, 20),
    #minutes_within_100m_50km_shoredist=time_in_shoredist_rng(
    #    track, zoneset, 0.1, 50),
)


def serialize_network_edge(tracks,
                           domain,
                           staticinfo=staticinfo,
                           transitinfo=transitinfo):
    ''' at each track position where the zone changes, a transit
        index is recorded, and trajectory statistics are aggregated for this
        index range using staticinfo() and transitinfo()

        results will be serialized as binary files labelled by mmsi into the
        'tmp_dir' directory, as defined in the config file. see graph() for
        deserialization and concatenation of results

        args:
            tracks: dict
                dictionary of vessel trajectory data, as output by
                ais.track_gen.trackgen() or its wrapper functions

        returns: None
    '''

    for track in tracks:
        filepath = os.path.join(tmp_dir, str(track['mmsi']).zfill(9))
        if not 'in_zone' in track.keys():
            get_zones = fence_tracks([track], domain=domain)
            track = next(get_zones)
            assert list(get_zones) == []

        with open(filepath, 'ab') as f:
            transits = np.where(
                track['in_zone'][:-1] != track['in_zone'][1:])[0] + 1

            for i in range(len(transits) - 1):
                rng = np.array(range(transits[i], transits[i + 1] + 1))
                track_stats = staticinfo(track)
                track_stats.update(transitinfo(track, rng))
                pickle.dump(track_stats, f)

            i0 = transits[-1] if len(transits) >= 1 else 0
            rng = np.array(range(i0, len(track['in_zone'])))
            track_stats = staticinfo(track)
            track_stats.update(transitinfo(track, rng))
            track_stats['rcv_zone'] = 'NULL'
            track_stats['transit_nodes'] = track_stats['src_zone']
            pickle.dump(track_stats, f)

        yield


def aggregate_output(filename='output.csv',
                     filters=[lambda row: False],
                     delete=True):
    ''' concatenate serialized output from geofence()

        filters: list of callables
            each callable function should accept a dictionary describing a
            network edge as input. if any of the callables return True,
            the edge will be filtered from the output rows. see staticinfo()
            and transitinfo() above for more info on network edge dict keys

            for example, to filter all rows where the max speed exceeds 50
            knots, and filter non-transiting vessels from zone Z0:

            >>> filters = [
                lambda row: row['velocity_knots_max'] == 'NULL' or float(row['velocity_knots_max']) > 50,
                lambda row: row['src_zone'] == 'Z0' and row['rcv_zone'] == 'NULL'
            ]
    '''

    outputfile = os.path.join(output_dir, filename)
    picklefiles = [
        os.path.join(tmp_dir, fname) for fname in sorted(os.listdir(tmp_dir))
        if '_' not in fname
    ]
    assert len(
        picklefiles
    ) > 0, 'failed to geofence any data... try running again with parallel=0'

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
            if delete: os.remove(picklefile)
            if len(results) == 0: continue
            output.write('\n'.join(results) + '\n')


def graph_cpu_bound(track, domain, **params):
    ''' will probably be removed in a later version '''
    #timesplit = partial(segment_tracks_timesplits, maxdelta=cuttime)
    distsplit = partial(segment_tracks_encode_greatcircledistance, **params)
    geofenced = partial(fence_tracks, domain=domain)
    #split_len = partial(max_tracklength,              max_track_length=10000)
    serialize = partial(serialize_network_edge, domain=domain)
    print('processing mmsi', track['mmsi'], end='\r')
    #list(serialize(geofenced(split_len(distsplit(timesplit([track]))))))
    #for t in serialize(geofenced(distsplit([track]))):
    #    pass
    list(serialize(geofenced(distsplit([track]))))
    #return


def graph_blocking_io(rowgen, domain):
    ''' will probably be removed in a later version '''
    #for x in merge_layers(trackgen(rowgen)):
    for x in trackgen(rowgen):
        yield x


def graph(rowgen, domain, parallel=0, **params):
    ''' perform geofencing on vessel trajectories, then concatenate aggregated
        transit statistics between nodes (zones) to create network edges from
        vessel trajectories

        this function will call geofence() for each trajectory in parallel,
        outputting serialized results to the tmp_dir directory. after
        deserialization, the temporary files are removed, and output will be
        written to 'output.csv' inside the data_dir directory

        args:
            rowgen: generator from aisdb.database.qrygen.DBQuery().gen_qry()
                see qrygen.py for more info
            domain: aisdb.gis.Domain() class object
                collection of zones defined as polygons, these will
                be used as nodes in the network graph
            parallel: integer
                number of processes to compute geofencing in parallel.
                if set to 0 or False, no parallelization will be used

        returns: None
    '''
    if not parallel:
        #for track in graph_blocking_io(fpath, domain):
        for track in graph_blocking_io(rowgen, domain):
            graph_cpu_bound(track, domain=domain, **params)
        print()

    else:
        with Pool(processes=parallel) as p:
            fcn = partial(graph_cpu_bound, domain=domain, **params)
            p.imap_unordered(
                #fcn, (tr for tr in graph_blocking_io(fpath, domain=domain)),
                fcn,
                (r for r in graph_blocking_io(rowgen, domain=domain)),
                chunksize=1)
            p.close()
            p.join()
        print()

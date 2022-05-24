''' collect vessel transits between zones (nodes), and aggregate various trajectory statistics '''

import os
import pickle
import re
import sqlite3
import tempfile
from datetime import timedelta
from functools import partial, reduce
from hashlib import sha256
from multiprocessing import Pool

import numpy as np

from aisdb.gis import (
    delta_knots,
    delta_meters,
    epoch_2_dt,
)
from aisdb.track_gen import (
    TrackGen,
    encode_greatcircledistance,
    fence_tracks,
    split_timedelta,
)
from aisdb.webdata.marinetraffic import vessel_info
from aisdb.webdata.merge_data import (
    merge_tracks_bathymetry,
    merge_tracks_shoredist,
)
from aisdb.proc_util import _segment_rng
from aisdb.wsa import wetted_surface_area

_colorhash = lambda mmsi: f'#{sha256(str(mmsi).encode()).hexdigest()[-6:]}'


def _depth_nonnegative(track, zoneset):
    ''' returns absolute value of bathymetric depths with topographic heights
        converted to 0
    '''
    return np.array(
        [d if d >= 0 else 0 for d in track['depth_metres'][zoneset]])


def _time_in_shoredist_rng(track, subset, dist0=0.01, dist1=5):
    ''' returns minutes spent within kilometers range from shore '''
    return sum(t for t in map(
        len,
        _segment_rng(
            {
                'time':
                track['time'][subset]
                [[dist0 <= d <= dist1 for d in track['km_from_shore'][subset]]]
            },
            maxdelta=timedelta(minutes=1),
            key='time'),
    ))


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


def _staticinfo(track, domain):
    ''' collect categorical vessel data as a dictionary '''
    return dict(
        mmsi=_sanitize(track['mmsi']),
        imo=_sanitize(track['marinetraffic_info']['imo']),
        vessel_name=_sanitize(track['marinetraffic_info']['name']),
        vesseltype_generic=_sanitize(
            track['marinetraffic_info']['vesseltype_generic']),
        vesseltype_detailed=_sanitize(
            track['marinetraffic_info']['vesseltype_detailed']),
        callsign=_sanitize(track['marinetraffic_info']['callsign']),
        flag=_sanitize(track['marinetraffic_info']['flag']),
        gross_tonnage=_sanitize(track['marinetraffic_info']['gross_tonnage']),
        summer_dwt=_sanitize(track['marinetraffic_info']['summer_dwt']),
        length_breadth=_sanitize(
            track['marinetraffic_info']['length_breadth']),
        year_built=_sanitize(track['marinetraffic_info']['year_built']),
        home_port=_sanitize(track['marinetraffic_info']['home_port']),
        error404=track['marinetraffic_info']['error404'],
        trackID=track["label"] if 'label' in track.keys() else '',
        hull_submerged_surface_area=f"{track['submerged_hull_m^2']:.0f}",
        #if 'submerged_hull_m^2' in track.keys() else '',
    )


_fstr = lambda s: f'{float(s):.2f}'


# collect aggregated statistics on vessel positional data
def _transitinfo(track, zoneset):
    ''' aggregate statistics on vessel network graph connectivity '''
    return dict(

        # geofencing
        src_zone=int(re.sub('[^0-9]', '', track['in_zone'][zoneset][0])),
        rcv_zone=int(re.sub('[^0-9]', '', track['in_zone'][zoneset][-1])),
        transit_nodes=
        f"{track['in_zone'][zoneset][0]}_{track['in_zone'][zoneset][-1]}",

        # timestamp info
        first_seen_in_zone=epoch_2_dt(
            track['time'][zoneset][0]).strftime('%Y-%m-%d %H:%M UTC'),
        last_seen_in_zone=epoch_2_dt(
            track['time'][zoneset][-1]).strftime('%Y-%m-%d %H:%M UTC'),
        year=epoch_2_dt(track['time'][zoneset][0]).year,
        month=epoch_2_dt(track['time'][zoneset][0]).month,
        day=epoch_2_dt(track['time'][zoneset][0]).day,

        # distance travelled
        total_distance_meters=np.sum(delta_meters(track,
                                                  zoneset[[0,
                                                           -1]])).astype(int),
        cumulative_distance_meters=np.sum(delta_meters(track,
                                                       zoneset)).astype(int),
        # shore dist
        #min_shore_dist=f"{np.min(track['km_from_shore'][zoneset]):.2f}",
        #avg_shore_dist=f"{np.average(track['km_from_shore'][zoneset]):.2f}"
        #if 'km_from_shore' in track.keys() else None,
        #max_shore_dist=f"{np.max(track['km_from_shore'][zoneset]):.2f}"
        #if 'km_from_shore' in track.keys() else None,

        # port dist
        #min_port_dist=_fstr(np.min(track['km_from_port'][zoneset])),
        #avg_port_dist=_fstr(np.average(track['km_from_port'][zoneset]))
        #if 'km_from_port' in track.keys() else None,
        #max_port_dist=_fstr(np.max(track['km_from_port'][zoneset]))
        #if 'km_from_port' in track.keys() else None,

        # depth charts
        #min_depth=_fstr(np.min(_depth_nonnegative(track, zoneset)))
        #if 'depth_metres' in track.keys() else None,
        #avg_depth=_fstr(np.average(_depth_nonnegative(track, zoneset)))
        #if 'depth_metres' in track.keys() else None,
        #max_depth=_fstr(np.max(_depth_nonnegative(track, zoneset)))
        #if 'depth_metres' in track.keys() else None,
        #avg_avg_depth_border_cells=_fstr(
        #    np.average(track['depth_border_cells_average'][zoneset]))
        #if 'depth_border_cells_average' in track.keys() else None,

        # computed velocity (knots)
        velocity_knots_min=f"{np.min(delta_knots(track, zoneset)):.2f}"
        if len(zoneset) > 1 else 'NULL',
        velocity_knots_avg=f"{np.average(delta_knots(track, zoneset)):.2f}"
        if len(zoneset) > 1 else 'NULL',
        velocity_knots_max=f"{np.max(delta_knots(track, zoneset)):.2f}"
        if len(zoneset) > 1 else 'NULL',

        # elapsed time spent in zones
        minutes_spent_in_zone=_fstr(
            (epoch_2_dt(track['time'][zoneset][-1]) -
             epoch_2_dt(track['time'][zoneset][0])).total_seconds() /
            60) if len(zoneset) > 1 else 'NULL',

        # elapsed time in distance from shore
        #minutes_within_10m_5km_shoredist=_time_in_shoredist_rng(
        #    track, zoneset, 0.01, 5),
        #minutes_within_30m_20km_shoredist=_time_in_shoredist_rng(
        #    track, zoneset, 0.03, 20),
        #minutes_within_100m_50km_shoredist=_time_in_shoredist_rng(
        #    track, zoneset, 0.1, 50),
    )


def _serialize_network_edge(tracks, domain, tmp_dir):
    ''' at each track position where the zone changes, a transit
        index is recorded, and trajectory statistics are aggregated for this
        index range using _staticinfo() and _transitinfo()

        results will be serialized as binary files labelled by mmsi into the
        'tmp_dir' directory, as defined in the config file. see graph() for
        deserialization and concatenation of results

        args:
            tracks: dict
                dictionary of vessel trajectory data, as output by
                ais.track_gen.TrackGen() or its wrapper functions

        returns: None
    '''
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

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
                track_stats = _staticinfo(track, domain)
                track_stats.update(_transitinfo(track, rng))
                pickle.dump(track_stats, f)

            i0 = transits[-1] if len(transits) >= 1 else 0
            rng = np.array(range(i0, len(track['in_zone'])))
            track_stats = _staticinfo(track, domain)
            track_stats.update(_transitinfo(track, rng))
            track_stats['rcv_zone'] = 'NULL'
            track_stats['transit_nodes'] = track_stats['src_zone']
            pickle.dump(track_stats, f)

        yield


def _aggregate_output(outputfile, tmp_dir, filters=[lambda row: False]):
    ''' concatenate serialized output from geofence()

        args:
            outputfile (string)
                filepath location to output CSV data
            tmp_dir (string)
                files will temporarily be placed here while processing
            filters (list)
                list of callback functions. each callable function should
                accept a dictionary describing a network edge as input. if any
                return True, the edge will be filtered from the output rows.
                see _staticinfo() and _transitinfo() for more info on
                network edge dict keys

                for example, to filter all rows where the max speed exceeds 50
                knots, and filter non-transiting vessels from zone Z0:

        >>> filters = [
        ...     lambda r: float(r['velocity_knots_max']) > 50,
        ...     lambda r: r['src_zone'] == '0' and r['rcv_zone'] == 'NULL'
        ...     ]
    '''

    picklefiles = [
        os.path.join(tmp_dir, fname) for fname in sorted(os.listdir(tmp_dir))
        if '_' not in fname
    ]
    assert len(
        picklefiles
    ) > 0, 'failed to geofence any data... try running again with processes=0'

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
                    except EOFError:
                        break
                    except Exception as e:
                        raise e
                    if not reduce(np.logical_or, [f(getrow) for f in filters]):
                        results.append(','.join(map(str, getrow.values())))

            if len(results) == 0:
                continue
            else:
                output.write('\n'.join(results) + '\n')

            os.remove(picklefile)


def _pipeline(track, *, domain, trafficDBpath, tmp_dir, maxdelta,
              distance_threshold, speed_threshold, minscore):
    trafficDB = sqlite3.Connection(trafficDBpath)
    trafficDB.row_factory = sqlite3.Row

    for x in _serialize_network_edge(
            # merge_tracks_shoredist(merge_tracks_bathymetry(
            fence_tracks(
                encode_greatcircledistance(
                    split_timedelta(
                        wetted_surface_area(
                            vessel_info([track], trafficDB=trafficDB), ),
                        maxdelta=maxdelta,
                    ),
                    distance_threshold=distance_threshold,
                    minscore=minscore,
                    speed_threshold=speed_threshold,
                ),
                domain=domain,
            ),
            # data_dir=data_dir), data_dir=data_dir),
            domain=domain,
            tmp_dir=tmp_dir,
    ):
        assert x is None


def graph(rowgen,
          *,
          domain,
          trafficDBpath,
          processes=0,
          outputfile='output.csv',
          maxdelta=timedelta(weeks=1),
          speed_threshold=50,
          distance_threshold=250000,
          minscore=0):
    ''' Compute network graph of vessel movements within domain zones.
        Zone polygons will be used as network nodes, with graph edges
        represented by movements between zones.

        args:
            rowgen (iter)
                :py:meth:`aisdb.database.dbqry.DBQuery.gen_qry` database
                query generator
            domain (:py:class:`aisdb.gis.Domain`)
                collection of zones defined as polygons, these will
                be used as nodes in the network graph
            processes (integer)
                number of processes to compute geofencing in parallel.
                if set to 0 or False, no parallelization will be used
            outpufile (string)
                filepath for resulting CSV output
            maxdelta (datetime.timedelta)
                maximum time between vessel messages before considering
                it to be part of a new trajectory. See
                :func:`aisdb.track_gen.split_timedelta` for more info
            speed_threshold (int, float)
                maximum speed in knots for encoder segmentation. See
                :func:`aisdb.track_gen.encode_greatcircledistance` for
                more info
            distance_threshold (int, float)
                maximum distance in meters for encoder segmentation. See
                :func:`aisdb.track_gen.encode_greatcircledistance` for
                more info
            minscore (float)
                minimum score for segments to be considered sequential. See
                :func:`aisdb.track_gen.encode_greatcircledistance` for
                more info

        Network graph activity is computed following these steps:

            - Create database query with
              :meth:`aisdb.database.dbqry.DBQuery.gen_qry`, and supply
              resulting generator as rowgen arg. Define a domain
              (:class:`aisdb.gis.Domain`) in which to compute movements
            - Vectorize tracks using :py:func:`aisdb.track_gen.TrackGen`
            - Append vessel metadata to track vessels with
              :func:`aisdb.webdata.marinetraffic.vessel_info`
            - Segment track vectors where time between messages exceeds
              maxdelta using :func:`aisdb.track_gen.split_timedelta`
            - Segment track vectors as encoded by
              :py:func:`aisdb.track_gen.encode_greatcircledistance`
            - Perform geofencing on track segments using
              :py:func:`aisdb.track_gen.fence_tracks` to determine zone
              containment
            - Check where zone boundaries are transited and serialize results
              to ``outputfile``. Additional metrics per zone activity is also
              aggregated at this step.

        Example usage:

        >>> from datetime import datetime
        >>> from aisdb import DBQuery, Domain, graph
        >>> from aisdb.database.sqlfcn_callbacks import in_bbox_time

        configure query area using Domain to compute region boundary

        >>> zones = [{
        ...     'name': 'Zone1',
        ...     'geometry': shapely.geometry.Polygon(zip(
        ...         [-170.24, -170.24, -38.5, -38.5, -170.24],
        ...         [29.0, 75.2, 75.2, 29.0, 29.0]))
        ...     }]
        >>> domain = Domain(name='new_domain', zones=zones)
        >>> trafficDBpath = './marinetraffic.db'

        query db for points in domain

        >>> qry = DBQuery(
        ...     callback=in_bbox_time,
        ...     start=datetime(2020, 9, 1),
        ...     end=datetime(2020, 9, 3),
        ...     **domain.boundary,
        ...     )
        >>> rowgen = qry.gen_qry()

        process the vessel movement graph edges using 12 processes in parallel

        >>> network_graph.graph(rowgen, domain=domain,
        ...                     trafficDBpath=trafficDBpath, parallel=12)
    '''
    with tempfile.TemporaryDirectory() as tmp_dir:
        fcn = partial(_pipeline,
                      domain=domain,
                      trafficDBpath=trafficDBpath,
                      tmp_dir=tmp_dir,
                      speed_threshold=speed_threshold,
                      distance_threshold=distance_threshold,
                      minscore=minscore,
                      maxdelta=maxdelta)

        if not processes or processes == 1:
            for track in TrackGen(rowgen):
                _ = fcn(track)

        else:
            with Pool(processes=processes) as p:
                p.imap_unordered(fcn, TrackGen(rowgen))
                p.close()
                p.join()

        if os.listdir(tmp_dir) == []:
            print(f'no data for {outputfile}, skipping...\n')
            return

        _aggregate_output(outputfile=outputfile, tmp_dir=tmp_dir)

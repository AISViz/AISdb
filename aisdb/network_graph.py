'''
    collect vessel transits between domain zones (graph nodes), and aggregate
    trajectory statistics within the overall domain
'''

import os
import pickle
import re
import tempfile
import types
from datetime import timedelta
from functools import partial, reduce

import numpy as np
import warnings

import aisdb
from aisdb.database import sqlfcn
from aisdb.gis import (
    delta_knots,
    delta_meters,
    epoch_2_dt,
)
from aisdb.track_gen import (
    TrackGen,
    fence_tracks,
    split_timedelta,
)
from aisdb.denoising_encoder import encode_greatcircledistance
from aisdb.database.dbconn import PostgresDBConn, SQLiteDBConn, ConnectionType
from aisdb.interp import interp_time
from aisdb.proc_util import _sanitize
from aisdb.proc_util import _segment_rng
from aisdb.webdata.bathymetry import Gebco
from aisdb.webdata.marinetraffic import vessel_info, VesselInfo
from aisdb.webdata.shore_dist import ShoreDist, PortDist
from aisdb.wsa import wetted_surface_area


def _fstr(s):
    return f'{float(s):.2f}'


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


def _staticinfo(track, domain):
    ''' collect categorical vessel data as a dictionary '''
    static = {'mmsi': track['mmsi']}
    for key in track['marinetraffic_info'].keys():
        if key in static.keys() or key not in track['marinetraffic_info'].keys(
        ):
            continue
        static.update({key: _sanitize(track['marinetraffic_info'][key])})
    for key in ['label', 'hull_submerged_m^2']:
        if key in static.keys() or key not in track.keys():
            continue
        static.update({key: _sanitize(f'{track[key]:.0f}')})
    return static


def _transitinfo(track, zoneset, interp_resolution=timedelta(hours=1)):
    ''' aggregate statistics on vessel network graph connectivity '''

    dynamic = {}

    # geofencing
    dynamic.update(
        dict(
            src_zone=int(re.sub('[^0-9]', '', track['in_zone'][zoneset][0])),
            rcv_zone=int(re.sub('[^0-9]', '', track['in_zone'][zoneset][-1])),
            transit_nodes=
            f"{track['in_zone'][zoneset][0]}_{track['in_zone'][zoneset][-1]}"))

    # timestamp info
    dynamic.update(
        dict(first_seen_in_zone=epoch_2_dt(
            track['time'][zoneset][0]).strftime('%Y-%m-%d %H:%M UTC'),
             last_seen_in_zone=epoch_2_dt(
                 track['time'][zoneset][-1]).strftime('%Y-%m-%d %H:%M UTC'),
             year=epoch_2_dt(track['time'][zoneset][0]).year,
             month=epoch_2_dt(track['time'][zoneset][0]).month,
             day=epoch_2_dt(track['time'][zoneset][0]).day))

    # distance travelled
    dynamic.update(
        dict(total_distance_meters=np.sum(delta_meters(
            track, zoneset[[0, -1]])).astype(int),
             cumulative_distance_meters=np.sum(delta_meters(
                 track, zoneset)).astype(int)))
    # shore dist
    if 'km_from_shore' in track.keys():
        dynamic.update(
            dict(
                min_shore_dist=f"{np.min(track['km_from_shore'][zoneset]):.2f}",
                avg_shore_dist=
                f"{np.average(track['km_from_shore'][zoneset]):.2f}"
                if 'km_from_shore' in track.keys() else None,
                max_shore_dist=f"{np.max(track['km_from_shore'][zoneset]):.2f}"
                if 'km_from_shore' in track.keys() else None,
            ))
        # elapsed time in distance from shore
        dynamic.update(
            dict(
                minutes_within_10m_5km_shoredist=_time_in_shoredist_rng(
                    track, zoneset, 0.01, 5),
                minutes_within_30m_20km_shoredist=_time_in_shoredist_rng(
                    track, zoneset, 0.03, 20),
                minutes_within_100m_50km_shoredist=_time_in_shoredist_rng(
                    track, zoneset, 0.1, 50),
            ))

    # port dist
    if 'km_from_port' in track.keys():
        dynamic.update(
            dict(
                min_port_dist=_fstr(np.min(track['km_from_port'][zoneset])),
                avg_port_dist=_fstr(np.average(track['km_from_port'][zoneset]))
                if 'km_from_port' in track.keys() else None,
                max_port_dist=_fstr(np.max(track['km_from_port'][zoneset]))
                if 'km_from_port' in track.keys() else None,
            ))

    # depth charts
    if 'depth_metres' in track.keys():
        dynamic.update(
            dict(
                min_depth=_fstr(np.min(_depth_nonnegative(track, zoneset)))
                if 'depth_metres' in track.keys() else None,
                avg_depth=_fstr(np.average(_depth_nonnegative(track, zoneset)))
                if 'depth_metres' in track.keys() else None,
                max_depth=_fstr(np.max(_depth_nonnegative(track, zoneset)))
                if 'depth_metres' in track.keys() else None,
            ))

    # computed velocity (knots)
    dynamic.update(
        dict(
            velocity_knots_min=f"{np.min(delta_knots(track, zoneset)):.2f}"
            if len(zoneset) > 1 else 'NULL',
            velocity_knots_avg=f"{np.average(delta_knots(track, zoneset)):.2f}"
            if len(zoneset) > 1 else 'NULL',
            velocity_knots_max=f"{np.max(delta_knots(track, zoneset)):.2f}"
            if len(zoneset) > 1 else 'NULL',
        ))

    # elapsed time spent in zones
    dynamic.update(
        dict(minutes_spent_in_zone=_fstr(
            (epoch_2_dt(track['time'][zoneset][-1]) -
             epoch_2_dt(track['time'][zoneset][0])).total_seconds() /
            60) if len(zoneset) > 1 else 'NULL', ))

    return dynamic


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
    for track in tracks:
        assert isinstance(track, dict)
        assert len(track['time']) > 0
        filepath = os.path.join(tmp_dir, str(track['mmsi']).zfill(9))
        assert 'in_zone' in track.keys(
        ), 'need to append zone info from fence_tracks'

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
    assert os.path.isdir(
        os.path.dirname(outputfile)), f'no directory for {outputfile}!'

    picklefiles = [
        os.path.join(tmp_dir, fname) for fname in sorted(os.listdir(tmp_dir))
        if '_' not in fname
    ]
    assert len(picklefiles) > 0, 'failed to geofence any data...'

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
                    if not reduce(np.logical_or,
                                  [f(getrow)
                                   for f in filters]):  # pragma: no cover
                        results.append(','.join(map(str, getrow.values())))

            if len(results) == 0:
                warnings.warn(f'no results for {outputfile}')
            else:
                output.write('\n'.join(results) + '\n')

            os.remove(picklefile)


def graph(
        qry,
        *,
        outputfile,
        domain,
        dbconn: ConnectionType,
        data_dir: str,
        trafficDBpath: str or None,  # none if using PostgresDBConn
        maxdelta: timedelta = timedelta(weeks=1),
        speed_threshold: float = 50,
        distance_threshold: float = 200000,
        interp_delta: float = timedelta(minutes=10),
        minscore: float = 0,
        qryfcn=sqlfcn.crawl_dynamic_static,
        bathy_dir: str = None,
        shoredist_raster: str = None,
        portdist_raster: str = None,
        decimate: float = 0.0001,
        verbose: bool = False):
    ''' Compute network graph of vessel movements within domain zones.
        Zone polygons will be used as network nodes, with graph edges
        represented by movements between zones.

        args:
            qry (:py:class:`aisdb.database.dbqry.DBQuery`)
                database query generator
            domain (:py:class:`aisdb.gis.Domain`)
                collection of zones defined as polygons, these will
                be used as nodes in the network graph
            dbconn (ConnectionType)
                Either a :class:`aisdb.database.dbconn.SQLiteDBConn` or
                :class:`aisdb.database.dbconn.PostgresDBConn` database
                connection objects
            data_dir (string)
                location of raster data
            trafficDBpath (string)
                path to marinetraffic database file
            outpufile (string)
                filepath for resulting CSV output
            maxdelta (datetime.timedelta)
                maximum time between vessel messages before considering
                it to be part of a new trajectory. See
                :func:`aisdb.track_gen.split_timedelta` for more info
            speed_threshold (int, float)
                maximum speed in knots for encoder segmentation. See
                :func:`aisdb.denoising_encoder.encode_greatcircledistance` for
                more info
            distance_threshold (int, float)
                maximum distance in meters for encoder segmentation. See
                :func:`aisdb.denoising_encoder.encode_greatcircledistance` for
                more info
            interp_delta (timedelta)
                track positions will be interpolated to the given sample rate
            minscore (float)
                minimum score for segments to be considered sequential. See
                :func:`aisdb.denoising_encoder.encode_greatcircledistance` for
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
              :py:func:`aisdb.denoising_encoder.encode_greatcircledistance`
            - Perform geofencing on track segments using
              :py:func:`aisdb.track_gen.fence_tracks` to determine zone
              containment
            - Check where zone boundaries are transited and serialize results
              to ``outputfile``. Additional metrics per zone activity is also
              aggregated at this step.

        Example usage:

        >>> import os
        >>> import shapely
        >>> from datetime import datetime
        >>> from aisdb import SQLiteDBConn, DBQuery, Domain, graph, decode_msgs
        >>> from aisdb.database.sqlfcn_callbacks import in_bbox_time

        >>> # create example database file
        >>> dbpath = './example.sqlitedb'
        >>> filepaths = ['./aisdb/tests/testdata/test_data_20210701.csv',
        ...              './aisdb/tests/testdata/test_data_20211101.nm4']
        >>> with SQLiteDBConn(dbpath) as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn,
        ...     source='TESTING', verbose=False)

        Next, configure query area using Domain to compute region boundary

        >>> zones = [{
        ...     'name': 'Zone1',
        ...     'geometry': shapely.geometry.Polygon(zip(
        ...             [-170.24, -170.24, -38.5, -38.5, -170.24],
        ...             [29.0, 75.2, 75.2, 29.0, 29.0],
        ...         ))
        ...     }]
        >>> domain = Domain(name='new_domain', zones=zones)
        >>> trafficDBpath = './testdata/marinetraffic_test.db'
        >>> data_dir = os.environ.get('AISDBDATADIR', '/tmp/ais/')

        Then, query db for points in domain

        >>> with SQLiteDBConn(dbpath) as dbconn:
        ...     qry = DBQuery(
        ...             dbconn=dbconn,
        ...             callback=in_bbox_time,
        ...             start=datetime(2021, 7, 1),
        ...             end=datetime(2021, 7, 3),
        ...             **domain.boundary,
        ...         )
        ...     graph(qry,
        ...           outputfile=os.path.join('testdata', 'test_graph.csv'),
        ...           dbconn=dbconn,
        ...           domain=domain,
        ...           data_dir=data_dir,
        ...           trafficDBpath=trafficDBpath)

        Afterwards, delete the example database file

        >>> os.remove(dbpath)
        >>> os.remove(os.path.join('testdata', 'test_graph.csv'))

        process the vessel movement graph edges.
        caution: this may consume a large amount of memory
    '''
    assert not isinstance(qry, types.GeneratorType),\
            'Got a generator for "qry" arg instead of DBQuery'

    assert isinstance(qry, aisdb.database.dbqry.DBQuery),\
            f'Not a DBQuery object! Got {qry}'

    if not isinstance(dbconn, (
            ConnectionType.SQLITE.value,
            ConnectionType.POSTGRES.value,
    )):
        raise ValueError("Invalid dbconn connection type")
    if isinstance(dbconn, ConnectionType.SQLITE.value):
        assert trafficDBpath is not None
        assert isinstance(trafficDBpath, str)
        vinfoDB = VesselInfo(trafficDBpath).trafficDB
    else:
        vinfoDB = dbconn

    rowgen = qry.gen_qry(fcn=qryfcn, verbose=verbose)
    tracks = TrackGen(rowgen, decimate)

    if portdist_raster is not None:
        pdist = PortDist(portdist_raster)
        with pdist:
            tracks = list(pdist.get_distance(tracks))

    if shoredist_raster is not None:
        sdist_dir, sdist_name = shoredist_raster.rsplit(os.path.sep, 1)
        sdist = ShoreDist(sdist_dir, sdist_name)
        with sdist:
            tracks = list(sdist.get_distance(tracks))

    if bathy_dir is not None:
        bathy = Gebco(data_dir=bathy_dir)
        with bathy:
            tracks = list(bathy.merge_tracks(tracks))

    # initialize raster data sources
    if not os.path.isdir('/tmp'):  # pragma: no cover
        os.mkdir('/tmp')
    with tempfile.TemporaryDirectory() as tmp_dir:
        if os.environ.get('DEBUG'):
            print(f'network graph {tmp_dir = }')
            print(f'\n{domain.name=} {domain.boundary=}')

        # configure processing pipeline
        serialize_CSV = partial(_serialize_network_edge,
                                domain=domain,
                                tmp_dir=tmp_dir)
        geofence = partial(fence_tracks, domain=domain)
        interp = partial(interp_time, step=interp_delta)
        encode_tracks = partial(encode_greatcircledistance,
                                distance_threshold=distance_threshold,
                                minscore=minscore,
                                speed_threshold=speed_threshold)
        timesplit = partial(split_timedelta, maxdelta=maxdelta)
        vinfo = partial(vessel_info, dbconn=vinfoDB)

        # pipeline execution order
        tracks = vinfo(tracks)
        tracks = wetted_surface_area(tracks)
        tracks = timesplit(tracks)
        tracks = encode_tracks(tracks)
        tracks = interp(tracks)
        tracks = geofence(tracks)
        result = serialize_CSV(tracks)

        for res in result:
            assert res is None

        if os.listdir(tmp_dir) == []:
            warnings.warn(f'no data for {outputfile}, skipping...\n')
        else:
            _aggregate_output(outputfile=outputfile, tmp_dir=tmp_dir)

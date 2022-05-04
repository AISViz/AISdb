import os
import logging

sqlpath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'aisdb_sql'))

from .database.create_tables import (
    sqlite_create_table_polygons,
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)

from .database.dbconn import DBConn

from .database.decoder import decode_msgs

from .database.dbqry import DBQuery

from .database import sqlfcn

from .database import sqlfcn_callbacks

from .webdata import merge_data

from .webdata.bathymetry import Gebco

from .webdata.shore_dist import shore_dist_gfw

from .webdata.merge_data import (
    merge_layers,
    merge_tracks_bathymetry,
    merge_tracks_shoredist,
)

from .gis import (
    Domain,
    DomainFromTxts,
    delta_knots,
    delta_meters,
    delta_seconds,
    distance3D,
    dt_2_epoch,
    epoch_2_dt,
    radial_coordinate_boundary,
    vesseltrack_3D_dist,
)

from .index import index

from .interp import (
    interp_time, )

from .network_graph import serialize_network_edge

from .proc_util import (
    fast_unzip,
    glob_files,
    read_binary,
    write_binary,
    write_csv,
)

from .track_gen import (
    TrackGen,
    split_timedelta,
    fence_tracks,
    max_tracklength,
    encode_greatcircledistance,
)

if not os.environ.get('SPHINXDOC'):
    from .aisdb import decode_native, haversine, simplify_linestring_idx

import sqlite3
if (sqlite3.sqlite_version_info[0] < 3
        or (sqlite3.sqlite_version_info[0] <= 3
            and sqlite3.sqlite_version_info[1] < 35)):
    import pysqlite3 as sqlite3

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO')
logging.basicConfig(format='%(message)s',
                    level=LOGLEVEL,
                    datefmt='%Y-%m-%d %I:%M:%S')

assert sqlite3.sqlite_version_info[
    0] >= 3, 'SQLite version too low! version 3.35 or newer required'
assert sqlite3.sqlite_version_info[
    1] >= 35, 'SQLite version too low! version 3.35 or newer required'

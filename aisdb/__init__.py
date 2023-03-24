import os
import logging
import warnings

import sqlite3
if (sqlite3.sqlite_version_info[0] < 3
        or (sqlite3.sqlite_version_info[0] <= 3
            and sqlite3.sqlite_version_info[1] < 8)):
    warnings.warn(
        f"An outdated version of SQLite was found ({sqlite3.sqlite_version})")

sqlpath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'aisdb_sql'))

from .database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)

from .database.dbconn import DBConn, SQLiteDBConn, PostgresDBConn

from .database.decoder import decode_msgs

from .database.dbqry import DBQuery

from .database import sqlfcn

from .database import sqlfcn_callbacks

from .webdata.bathymetry import Gebco

from .webdata.shore_dist import ShoreDist, PortDist

from .gis import (
    Domain,
    DomainFromTxts,
    DomainFromPoints,
    delta_knots,
    delta_meters,
    delta_seconds,
    distance3D,
    dt_2_epoch,
    epoch_2_dt,
    radial_coordinate_boundary,
    vesseltrack_3D_dist,
)

from .interp import (
    interp_time, )

from .network_graph import graph

from .receiver import start_receiver

from .proc_util import (
    glob_files,
    write_csv,
)

from .track_gen import (
    TrackGen,
    split_timedelta,
    fence_tracks,
    encode_score,
    encode_greatcircledistance,
)

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO')
logging.basicConfig(format='%(message)s',
                    level=LOGLEVEL,
                    datefmt='%Y-%m-%d %I:%M:%S')

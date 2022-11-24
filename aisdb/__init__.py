import os
import logging

import pysqlite3 as sqlite3

sqlpath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'aisdb_sql'))

from .database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)

from .database.dbconn import DBConn

from .database.decoder import decode_msgs

from .database.dbqry import DBQuery, DBQuery_async

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
    TrackGen_async,
    split_timedelta,
    fence_tracks,
    encode_greatcircledistance,
    encode_greatcircledistance_async,
)

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO')
logging.basicConfig(format='%(message)s',
                    level=LOGLEVEL,
                    datefmt='%Y-%m-%d %I:%M:%S')

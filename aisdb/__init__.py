import logging
import os
import warnings
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("aisdb")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

import sqlite3

if sqlite3.sqlite_version_info[0] < 3 or (
    sqlite3.sqlite_version_info[0] <= 3 and sqlite3.sqlite_version_info[1] < 8
):
    warnings.warn(f"An outdated version of SQLite was found ({sqlite3.sqlite_version})")

sqlpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "aisdb_sql"))

import aisdb.web_interface
from aisdb.web_interface import visualize

from .database.decoder import decode_msgs

from .database.dbconn import DBConn, SQLiteDBConn, PostgresDBConn

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
    mask_in_radius_2D,
    radial_coordinate_boundary,
    vesseltrack_3D_dist,
)

from .interp import (
    geo_interp_time,
    interp_cubic_spline,
    interp_spacing,
    interp_time,
)

from .network_graph import graph

from .receiver import start_receiver

from .proc_util import (
    glob_files,
    write_csv,
    write_csv_rows,
)

from .track_gen import (
    TrackGen,
    fence_tracks,
    min_speed_filter,
    split_timedelta,
    split_tracks,
)
from .denoising_encoder import (
    encode_score,
    encode_greatcircledistance,
    remove_pings_wrt_speed,
)

from .weather.data_store import WeatherDataStore
from .discretize.h3 import Discretizer

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO")
logging.basicConfig(format="%(message)s", level=LOGLEVEL, datefmt="%Y-%m-%d %I:%M:%S")

from .ports.api import WorldPortIndexClient

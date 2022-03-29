import os
import sys
import configparser
import logging

LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO')
logging.basicConfig(format='%(message)s',
                    level=LOGLEVEL,
                    datefmt='%Y-%m-%d %I:%M:%S')

sys.path.append(os.path.dirname(__file__))
pkgname = 'ais'
cfgfile = os.path.join(os.path.expanduser('~'), '.config', f'{pkgname}.cfg')

# default config values
data_dir = os.path.join(os.path.expanduser('~'), f'{pkgname}') + os.path.sep
dbpath = os.path.join(data_dir, 'ais.db')
tmp_dir = os.path.join(data_dir, 'tmp_parsing') + os.path.sep
zones_dir = os.path.join(data_dir, 'zones') + os.path.sep
rawdata_dir = os.path.join(data_dir, 'rawdata') + os.path.sep
host_addr = 'localhost'
host_port = 9999
output_dir = os.path.join(data_dir, 'scriptoutput') + os.path.sep
# marinetraffic_VD02_key = ''

# common imports that should be shared with module subdirectories
commondirs = ['.', 'database', 'webdata']
cfgnames = [
    'data_dir',
    'dbpath',
    'tmp_dir',
    'zones_dir',
    'rawdata_dir',
    'output_dir',
    'host_addr',
    'host_port',
    # 'marinetraffic_VD02_key',
]

sqlpath = os.path.abspath(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), 'aisdb_sql'))


def printdefault(cfgnames, quote=''):
    return '\n'.join([f'{c} = {quote}{eval(c)}{quote}' for c in cfgnames])


# read config file
if os.path.isfile(cfgfile):

    cfg = configparser.ConfigParser()
    try:
        with open(cfgfile, 'r') as f:
            cfg.read_string('[DEFAULT]\n' + f.read())
        if len(list(cfg.keys())) > 1:
            raise KeyError('Error in config file: wrong number of sections')
    except configparser.Error as err:
        print('could not read the configuration file!\n')
        raise err.with_traceback(None)
    settings = dict(cfg['DEFAULT'])

    # initialize config settings as variables
    for setting in cfgnames:
        exec(f'''{setting} = settings['{setting.lower()}'] '''
             f'''if '{setting.lower()}' in settings.keys() else {setting}''')
        if setting[-4:] == '_dir' and not os.path.isdir(settings[setting]):
            print(f'creating directory {settings[setting]}')
            os.mkdir(settings[setting])

    # convert port string to integer
    if isinstance(host_port, str):
        assert host_port.isnumeric(
        ) and float(host_port) % 1 == 0, 'host_port must be an integer value'
        host_port = int(host_port)

else:
    print(
        f'''\n{printdefault(cfgnames)}\n\n'''
        f'''no .cfg file found, writing default configuration to {cfgfile}''')
    if not os.path.isdir(os.path.dirname(cfgfile)):
        os.mkdir(os.path.dirname(cfgfile))
    with open(cfgfile, 'w') as f:
        f.write(printdefault(cfgnames))
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)
    # if not os.path.isdir(tmp_dir):
    #    os.mkdir(tmp_dir)


class import_handler():

    def __init__(self):
        self.commonpaths = [
            os.path.join(os.path.dirname(__file__), dirname, 'common.py')
            for dirname in commondirs
        ]

    def __enter__(self):
        common = printdefault(cfgnames, quote="'")
        for fpath in self.commonpaths:
            with open(fpath, 'w') as f:
                f.write(common)

    def __exit__(self, exc_type, exc_value, tb):
        for fpath in self.commonpaths:
            os.remove(fpath)


with import_handler() as importconfigs:

    from .version import __version__

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
        merge_tracks_hullgeom,
        merge_tracks_shoredist,
    )

    from .gis import (
        Domain,
        ZoneGeom,
        ZoneGeomFromTxt,
        delta_knots,
        delta_meters,
        delta_reported_knots,
        delta_seconds,
        distance3D,
        dms2dd,
        dt_2_epoch,
        epoch_2_dt,
        haversine,
        radial_coordinate_boundary,
        strdms2dd,
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

    from .wsa import wsa

import sqlite3
if (sqlite3.sqlite_version_info[0] < 3
        or (sqlite3.sqlite_version_info[0] <= 3
            and sqlite3.sqlite_version_info[1] < 35)):
    import pysqlite3 as sqlite3

assert sqlite3.sqlite_version_info[
    0] >= 3, 'SQLite version too low! version 3.35 or newer required'
assert sqlite3.sqlite_version_info[
    1] >= 35, 'SQLite version too low! version 3.35 or newer required'

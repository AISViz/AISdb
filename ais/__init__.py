import os
import sys
#import shlex
import configparser


sys.path.append(os.path.dirname(__file__))
pkgname = 'ais'
cfgfile = os.path.join(os.path.expanduser('~'), '.config', f'{pkgname}.cfg')



dbpath = os.path.join(os.path.expanduser('~'), f'{pkgname}.db')
data_dir = os.path.join(os.path.expanduser('~'), f'{pkgname}') + os.path.sep
tmp_dir = os.path.join(data_dir, 'tmp_parsing') + os.path.sep 
zones_dir = os.path.join(data_dir, 'zones') + os.path.sep 
rawdata_dir = os.path.join(data_dir, 'rawdata') + os.path.sep

if os.path.isfile(cfgfile):
    cfg = configparser.ConfigParser()
    with open(cfgfile, 'r') as f:
        cfg.read_string('[DEFAULT]\n' + f.read())

    settings = dict(cfg['DEFAULT'])

    dbpath = settings['dbpath']             if 'dbpath'      in settings.keys() else dbpath
    data_dir = settings['data_dir']         if 'data_dir'    in settings.keys() else data_dir
    tmp_dir = settings['tmp_dir']           if 'tmp_dir'     in settings.keys() else tmp_dir
    zones_dir = settings['zones_dir']       if 'zones_dir'   in settings.keys() else zones_dir
    rawdata_dir = settings['rawdata_dir']   if 'rawdata_dir' in settings.keys() else zones_dir

else:

    printdefault = lambda names, vals: '\n'.join([f'{n} = {v}' for n, v in zip(names, vals)])

    print(f'''no config file found, applying default configs:\n\n{
    printdefault(names=['dbpath', 'data_dir', 'tmp_dir', 'zones_dir',  'rawdata_dir'], 
                 vals=[dbpath, data_dir, tmp_dir, zones_dir,  rawdata_dir])
    }\n\nto remove this warning, copy and paste the above text to {cfgfile} ''')

#import .track_geom

from .database import *
#import .gis
from .gis import *
#import .track_gen 
from .track_gen import *

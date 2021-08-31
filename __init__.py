import os

pkgname = 'ais'
cfgfile = os.path.join(os.path.expanduser('~'), '.config', f'{pkgname}.cfg')

if os.path.isfile(cfgfile):
    exec(open(cfgfile).read())
else:
    default = f'''\n
    import os
    dbpath = os.path.join(os.path.expanduser('~'), f'{pkgname}.db')
    data_dir = os.path.join(os.path.expanduser('~'), f'{pkgname}') + os.path.sep
    zones_dir = os.path.join(data_dir, 'zones') + os.path.sep '''

    print(f'no config file found at {cfgfile}. applying default configs: {default}')
    exec(default)
#import .track_geom

#import .database
from database import *
#import .gis
from gis import *
#import .track_gen 
from track_gen import *
#from .track_geom import *
#from .test import *

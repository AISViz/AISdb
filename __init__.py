import os
import shlex

pkgname = 'ais'
cfgfile = os.path.join(os.path.expanduser('~'), '.config', f'{pkgname}.cfg')

if os.path.isfile(cfgfile):

    with open(cfgfile) as f:
        cfgs = shlex.shlex(instream=f, posix=False, punctuation_chars=True)
        cfgs.whitespace = '\t\r\n'
        cfgs.whitespace_split=True
        cmd = True

        while cmd != '':
            cmd = cfgs.read_token()
            exec(cmd)

else:
    default = f'''\n
    dbpath = os.path.join(os.path.expanduser('~'), f'{pkgname}.db')
    data_dir = os.path.join(os.path.expanduser('~'), f'{pkgname}') + os.path.sep
    zones_dir = os.path.join(data_dir, 'zones') + os.path.sep '''

    printdefault = lambda s: '\n'.join(map(
        lambda l: l.split('=')[0] + '= "' + eval(l.split('=')[1]) + '"', 
        s.replace('\n\n','').split('\n')
    ))

    print(f'no config file found at {cfgfile}. applying default configs:\n\n{printdefault(default)}')
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

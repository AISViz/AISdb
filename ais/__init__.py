import os
import sys
import shlex


sys.path.append(os.path.dirname(__file__))
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
tmp_dir = os.path.join(data_dir, 'tmp_parsing') + os.path.sep 
zones_dir = os.path.join(data_dir, 'zones') + os.path.sep 
'''

    printdefault = lambda s: '\n'.join(map(
        lambda l: l.split('=')[0] + '= "' + eval(l.split('=')[1]) + '"', 
        s.replace('\n\n','').split('\n')
    ))

    exec(default)
    print(f'''# no config file found, applying default configs:\n\n{printdefault(default)}\n
# to remove this warning, copy and paste this text to {cfgfile}''')

#import .track_geom

from .database import *
#import .gis
from .gis import *
#import .track_gen 
from .track_gen import *

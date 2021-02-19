import os
from os.path import sep 
import sys
import tarfile
import requests
import subprocess
import shutil

subprocess.run(f'''{sys.executable} -m pip install numpy shapely pyais requests''', shell=True)

"""
python configured with following compile flags

#./configure --enable-loadable-sqlite-extensions --enable-profiling --enable-optimizations --enable-ipv6 --enable-shared --with-lto --with-address-sanitizer --with-memory-sanitizer

./configure --enable-loadable-sqlite-extensions --enable-optimizations --disable-ipv6
"""


if not os.path.isdir(f'database{sep}libs'): os.mkdir(f'database{sep}libs')

req = requests.get('http://www.gaia-gis.it/gaia-sins/libspatialite-5.0.1.tar.gz', stream=True)
with open(tar := f'database{sep}libs{sep}libspatialite.tar.tgz', 'wb') as f:
    list(map(lambda chunk: f.write(chunk), req.iter_content(chunk_size=1024)))

tarfile.open(tar).extractall(f'database{sep}libs{sep}')


os.chdir(f'database{sep}libs{sep}libspatialite-5.0.1')

subprocess.run('sudo -E apt-get install libproj-dev')

subprocess.run('./configure --disable-rttopo'.split())
subprocess.run('make')
subprocess.run('sudo -E make install'.split())

import sqlite3
conn = sqlite3.connect(':memory:')
cur = conn.cursor()
conn.enable_load_extension(True)

assert conn.execute('SELECT load_extension("mod_spatialite.so")')
assert cur.fetchall() == []



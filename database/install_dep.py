import os
from os.path import sep 
import sys
import tarfile
import requests
import subprocess
import shutil


req = requests.get('https://www.python.org/ftp/python/3.9.2/Python-3.9.2.tgz', stream=True)
with open(tar := f'database{sep}libs{sep}Python-3.9.2.tgz', 'wb') as f:
    list(map(lambda chunk: f.write(chunk), req.iter_content(chunk_size=1024)))

tarfile.open(tar).extractall(f'database{sep}libs{sep}')
os.chdir(f'database{sep}libs{sep}Python-3.9.2')

subprocess.run('./configure --enable-optimizations --with-lto --disable-ipv6 --enable-loadable-sqlite-extensions'.split())

if not os.path.isdir(f'database{sep}libs'): os.mkdir(f'database{sep}libs')
req = requests.get('http://www.gaia-gis.it/gaia-sins/libspatialite-5.0.1.tar.gz', stream=True)
with open(tar := f'database{sep}libs{sep}libspatialite.tar.tgz', 'wb') as f:
    list(map(lambda chunk: f.write(chunk), req.iter_content(chunk_size=1024)))

tarfile.open(tar).extractall(f'database{sep}libs{sep}')


os.chdir(f'database{sep}libs{sep}libspatialite-5.0.1')

subprocess.run('sudo -E apt-get install libproj-dev')

subprocess.run('./configure --disable-rttopo'.split())
subprocess.run('make')
subprocess.run('sudo -E make install-strip'.split())

import sqlite3
conn = sqlite3.connect(':memory:')
cur = conn.cursor()
conn.enable_load_extension(True)

assert conn.execute('SELECT load_extension("mod_spatialite.so")')
assert cur.fetchall() == []
conn.execute('SELECT InitSpatialMetaData(1);') 


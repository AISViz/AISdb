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

subprocess.run('./configure --'.split())


"""
#  compile openssl from source https://wiki.openssl.org/index.php/Compilation_and_Installation
      ./config -Werror -Wl,-rpath=/usr/local/ssl/lib --static -static --prefix=/opt/openssl --openssldir=/usr/local/ssl no-ssl2 no-ssl3 shared no-idea no-asm no-hw no-engine no-err no-weak-ssl-ciphers no-asan # --strict-warnings 
      make update
      make 
      sudo make install

subprocess.run(f'''{sys.executable} -m pip install numpy shapely pyais requests''', shell=True)


python configured with following compile flags

#./configure --enable-loadable-sqlite-extensions --enable-profiling --enable-optimizations --enable-ipv6 --enable-shared --with-lto --with-address-sanitizer --with-memory-sanitizer
#./configure --enable-loadable-sqlite-extensions --enable-optimizations --disable-ipv6
./configure --with-openssl=/opt/openssl --enable-optimizations --with-lto --disable-ipv6 --enable-loadable-sqlite-extensions
"""
# V1

if not os.path.isdir('aislib'): 
    os.mkdir('aislib')
    import requests
    import tarfile
    req = requests.get('http://www.gaia-gis.it/gaia-sins/libspatialite-5.0.0.tar.gz')
    with open(tarpath:='aislib/libspatialite-5.0.0.tar.gz', 'wb') as f: f.write(req.content)
    tar = tarfile.open(tarpath, 'r:gz')
    tar.extractall('aislib/')
    os.remove(tarpath)
    os.chdir('aislib/libspatialite-5.0.0/')
    os.system(f'./configure --prefix={os.path.abspath("..")}')
    os.system('make')
    os.system('make install-strip')
    os.chdir('../..')


# V2 alternate 

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






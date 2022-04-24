import os
import sys
import pip
import shutil
import subprocess
from setuptools import setup

thispath = os.path.dirname(__file__)

# upgrade pip if necessary
majorver = int(pip.__version__.split('.')[0])
if majorver < 21:
    print('pip version too low! pip will now be upgraded')
    os.system(f'{sys.executable} -m pip install --upgrade pip numpy wheel')
    from importlib import reload
    reload(pip)

# compile rust target
cargopath = shutil.which('cargo')
if cargopath:
    projpath = os.path.join(thispath, 'aisdb_rust', 'Cargo.toml')
    subprocess.run(
        f'{cargopath} build --manifest-path={projpath} --release'.split())

# parse pkg version file
versionfile = os.path.join(thispath, 'aisdb', 'version.py')
with open(versionfile, 'r') as f:
    pkgversion = f.read()
exec(pkgversion)

setup(
    name='aisdb',
    version=__version__,
    description='AIS Database and Processing Utils',
    author='Matt Smith',
    author_email='matthew.smith@dal.ca',
    url='https://gitlab.meridian.cs.dal.ca/public_projects/aisdb',
    license='GNU General Public License v3.0',
    python_requires='>=3.8',
    packages=[
        'aisdb',
        'aisdb.database',
        'aisdb.webdata',
        'aisdb_rust',
        'aisdb_sql',
    ],
    package_data={'aisdb_sql': []},
    include_package_data=True,
    setup_requires=[
        'numpy',
        'pip>=21.1.0',
        'wheel',
    ],
    install_requires=[
        'aiosqlite',
        'numpy',
        'packaging',
        'pillow',
        'pip>=21.1.0',
        'pysqlite3',
        'requests',
        'selenium',
        'shapely',
        'tqdm',
        'websockets',
    ],
    extras_require={
        'testing': [
            'pytest',
            'pytest-asyncio',
        ],
        'docs': [
            'sphinx',
        ],
    },
)

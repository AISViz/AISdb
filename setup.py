import os
import sys
import ensurepip

ensurepip.bootstrap(upgrade=True)
import pip
from setuptools import setup
import shutil
import subprocess

majorver = int(pip.__version__.split('.')[0])
if majorver < 21:
    print('pip version too low! pip will now be upgraded')
    os.system(f'{sys.executable} -m pip install --upgrade pip numpy wheel')
    from importlib import reload
    reload(pip)

cargopath = shutil.which('cargo')
thispath = os.path.dirname(__file__)
if cargopath:
    projpath = os.path.join(thispath, 'aisdb_rust', 'Cargo.toml')
    subprocess.run(
        f'{cargopath} build --manifest-path={projpath} --release'.split())

setup(
    name='aisdb',
    version='0.1',
    description='AIS Database and Processing Utils',
    author='Matt Smith',
    author_email='matthew.smith@dal.ca',
    url='https://gitlab.meridian.cs.dal.ca/matt_s/ais_public',
    license='GNU General Public License v3.0',
    python_requires='>=3.8',
    packages=[
        'aisdb',
        'aisdb.database',
        'aisdb.webdata',
        'aisdb_rust',
    ],
    setup_requires=[
        'numpy',
        'pip>=21.1.0',
        'wheel',
    ],
    install_requires=[
        'numpy',
        'packaging',
        'pillow',
        'pip>=21.1.0',
        'pyais>=1.6.1',
        'requests',
        'selenium',
        'shapely',
        'tqdm',
    ],
    extras_require={
        'testing': [
            'pytest',
        ],
        'monitor': [
            'pytest',
            'pytest-monitor',
        ],
        'docs': [
            'sphinx',
        ],
    },
)

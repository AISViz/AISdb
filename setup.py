import os
import shutil
import subprocess
from setuptools import setup
from setuptools_rust import Binding, RustExtension

thispath = os.path.dirname(__file__)

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
# create __version__ definition
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
        'aisdb_extra',
    ],
    package_data={'aisdb_sql': []},
    include_package_data=True,
    setup_requires=[
        'numpy',
        'wheel',
        'setuptools-rust>=0.10.1',
    ],
    rust_extensions=[
        RustExtension("aisdb_extra.aisdb_extra",
                      os.path.join('aisdb_extra', 'Cargo.toml'),
                      binding=Binding.PyO3)
    ],
    install_requires=[
        'aiosqlite',
        'numpy',
        'packaging',
        'pillow',
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
    zip_safe=False,
)

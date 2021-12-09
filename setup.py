from setuptools import setup, find_packages
import ensurepip
from packaging import version

if version.parse(ensurepip.version()) < version.parse('21.1.0'):
    print('pip version too low! pip will now be upgraded')
    ensurepip.bootstrap(upgrade=True)
    print('pip successfully upgraded, please try installing again')
    exit()


setup(name='aisdb',
      version='0.1',
      description='AIS Database and Processing Utils',
      author='Matt Smith',
      author_email='matthew.smith@dal.ca',
      url='https://gitlab.meridian.cs.dal.ca/matt_s/ais_public',
      license='GNU General Public License v3.0',
      python_requires='>=3.8',
      packages=[
          'aisdb', 'aisdb.database', 'aisdb.webdata', 
        ],
      #packages=find_packages(exclude=['tests', 'docs', 'output', '__pycache__', 'tonnage.db']),
      setup_requires=[
          'cython',
          #'setuptools',
          'wheel',
        ],
      install_requires=[
          'pip>=21.1.0',
          'numpy',
          'pysqlite3-binary', 
          'pyais', 
          'shapely', 
          'requests', 
          'rasterio', 
          'packaging', 
          'selenium', 
          'tqdm',
          'scikit-learn',
        ],
      tests_requires=[
          'pytest',
          #'pytest-monitor',
          'sphinx', # required for building docs
        ],
     )


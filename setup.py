from setuptools import setup 

setup(name='ais',
      version='0.1',
      description='Tools for storing, processing, and analyzing AIS data',
      author='Matt Smith',
      author_email='matthew.smith@dal.ca',
      url='https://gitlab.meridian.cs.dal.ca/matt_s/ais_public',
      license='GNU General Public License v3.0',
      python_requires='>=3.6',
      packages=[
          '.', '.database', '.webdata',
        ],
      install_requires=[
          'numpy',
          'pysqlite3-binary', 
          'pyais', 
          'shapely', 
          'requests', 
          'rasterio', 
          'packaging', 
          'selenium', 
          'tqdm',
        ],
     )


from setuptools import setup, find_packages

setup(name='aisdb',
      version='0.1',
      description='AIS Database and Processing Utils',
      author='Matt Smith',
      author_email='matthew.smith@dal.ca',
      url='https://gitlab.meridian.cs.dal.ca/matt_s/ais_public',
      license='GNU General Public License v3.0',
      python_requires='>=3.6',
      packages=[
          'aisdb', 'aisdb.database', 'aisdb.webdata', 
        ],
      #packages=find_packages(exclude=['tests', 'docs', 'output', '__pycache__', 'tonnage.db']),
      setup_requires=[
          'cython',
          'setuptools',
          'wheel',
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
          'scikit-learn',
          'sphinx',
        ],
     )


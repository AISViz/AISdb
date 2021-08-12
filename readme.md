Functions and utilities for the purpose of decoding, storing, accessing, and processing AIS data. 

Setup:
  ```
  python3 -m pip install --upgrade numpy pysqlite3-binary pyais shapely requests rasterio packaging selenium tqdm
  ```

Requirements:
  * Python 3
  * SQLite >= 3.35
  * NumPy
  * pyais

Optional:
  * qgis
  * Shapely
  * requests
  * selenium
  * rasterio



![ais tracks - one month in the canadian atlantic](https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/output/scriptoutput.png)



## Overview of package components

  - database/
    - __init__.py             import statements  
    - create_tables.py        SQL database schematics and triggers. used in decoder.py
    - dbconn.py               exposes the SQLite DB connection. some postgres code is also included for legacy purposes
    - decoder.py              parsing .NMEA messages to create an SQL database. See function parallel_decode()
    - install_dep.py          will probably be removed in the future. contains code for compiling python from source
    - lambdas.py              contains useful one-liners and lambda functions. notably includes DB query callback functions
    - qryfcn.py               SQL boilerplate for dynamically creating database queries. used when calling qrygen.py
    - qrygen.py               class to convert a dictionary of input parameters into SQL code, and generate queries
  
  
  - webdata/
    - marinetraffic.py        scrape vessel information such as deadweight tonnage from marinetraffic.com
    - scraper.py              a general-use webscraper utilizing selenium, firefox, and mozilla geckodriver
  

  - __init__.py               import statements
  - gebco.py                  load bathymetry data from GEBCO raster files
  - gis.py                    geometry and GIS related utilities
  - index.py                  job scheduler and hashmap database utility, used to parallelize functions and store arbitrary binary
  - interp.py                 linear interpolation of track segments on temporal axis
  - rowexplore.py             crawl the entire database for rows matching certain conditions
  - shore_dist.py             collect shore distance at a given coordinates using GFW distance raster
  - track_gen.py              generation, segmentation, and filtering of vessel trajectories
  - track_viz.py              vizualize AIS data and geometry features using QGIS. should be considered experimental
  - wsa.py                    compute wetted surface area using denny-mumford regression on vessel deadweight tonnage
  - zonecrossing.py           collect vessel transits between regions of interest
   


## Getting Started

** Parsing raw NMEA messages into a database
How to generate an SQLite database from NMEA binary messages:

```
from database import parallel_decode

# example
filepaths = ['/home/matt/ais/NMEA_20210101.nm4', '/home/matt/ais/NMEA_20210102.nm4', ] # list should include ALL messages available for the given month(s) of data
dbpath = '/home/matt/ais.db'  # location of where the database file will be stored
processes = 12  # number of parallel process workers to use

parallel_decode(filepaths, dbpath, processes)

```

Some points to note when decoding: 
  - Only dynamic position reports (messages 1, 2, 3, 18, 19) and static reports (messages 5, 24) will be kept.
  - All other message types will be discarded. Support for long range position reports (message 27) may be added in the future.
  - Temporal resolution will be reduced to one message per MMSI per minute. The first occurring message will be kept
  - Decoding, deduplicating, and downscaling will be done in parallel. The actual database insertion and subsequent aggregation steps will be performed sequentially (I/O bound)


Acknowledgement:
  * Database schema has been adapted from the postgres data model developed by Casey Hilliard


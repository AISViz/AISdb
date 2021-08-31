Functions and utilities for the purpose of decoding, storing, processing, and visualizing AIS data. 

Install using pip:
  ```
  python3 -m pip install git+https://gitlab.meridian.cs.dal.ca/matt_s/ais_public#egg=ais
  ```

Optional for visualizing data:
  * qgis (must be in PYTHONPATH)



## Getting Started

### Parsing raw NMEA messages into a database


```
import os
from ais.database import parallel_decode

filepaths = os.listdir('/home/matt/ais_raw_NMEA/')    # filepaths to .nm4 message reports (list of strings)
dbpath = '/home/matt/ais.db'                          # location of where the database file will be stored
processes = 12                                        # number of processes to run in parallel. set to False to disable paralellizing

parallel_decode(filepaths, dbpath, processes)
```

Some points to note when decoding: 
  - Only position reports (messages 1, 2, 3, 18, 19) and static vessel reports (messages 5, 24) will be kept. All other messages are discarded.
  - Support for long range position reports (message 27) may be added in the future.
  - Temporal resolution will be reduced to one message per MMSI per minute. The first occurring message will be kept.
  - Decoding, deduplicating, and downscaling will be done in parallel. The actual database insertion and subsequent aggregation steps will be performed sequentially (I/O bound).  


### Querying the database  
  
  
##### Table naming conventions
  TODO: add documentation  


##### Dynamically generating SQL queries from a dictionary of input parameters  
  TODO: add documentation  


## Overview of package components

<pre>
database/
  __init__.py             import statements  
  create_tables.py        SQL database schematics and triggers. used in decoder.py
  dbconn.py               exposes the SQLite DB connection. some postgres code is also included for legacy 
  decoder.py              parsing .NMEA messages to create an SQL database. See function parallel_decode()
  install_dep.py          will probably be removed in the future. contains code for compiling python from source
  lambdas.py              contains useful one-liners and lambda functions. notably includes DB query callback functions
  qryfcn.py               SQL boilerplate for dynamically creating database queries. used when calling qrygen.py
  qrygen.py               class to convert a dictionary of input parameters into SQL code, and generate queries

webdata/
  marinetraffic.py        scrape vessel information such as deadweight tonnage from marinetraffic.com
  scraper.py              a general-use webscraper utilizing selenium, firefox, and mozilla geckodriver

__init__.py               import statements
gebco.py                  load bathymetry data from GEBCO raster files
gis.py                    geometry and GIS related utilities
index.py                  job scheduler and hashmap database utility, used to parallelize functions and store arbitrary binary
interp.py                 linear interpolation of track segments on temporal axis
network_graph.py          collect vessel transits between zones (nodes), and aggregate various trajectory statistics
rowexplode.py             crawl the database for rows matching certain conditions
shore_dist.py             collect shore distance at a given coordinates using GFW distance raster
track_gen.py              generation, segmentation, and filtering of vessel trajectories
track_viz.py              vizualize AIS data and geometry features using QGIS. should be considered experimental
wsa.py                    compute wetted surface area using denny-mumford regression on vessel deadweight tonnage
</pre> 


## Acknowledgement
  - Database schema has been adapted from the postgres data model developed by Casey Hilliard


![ais tracks - one month in the canadian atlantic](https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/output/scriptoutput.png)


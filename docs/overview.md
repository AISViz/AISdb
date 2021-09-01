## Overview
A more in-depth description of package components for SQL programmers and package contributors

  
### Table naming conventions
  TODO: add documentation  


### Overview of package components

<pre>
ais/
  __init__.py               import statements, load configs
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
</pre> 



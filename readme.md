<img src="https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/tests/output/scriptoutput.png" alt="ais tracks - one month in the canadian atlantic" width="900"/>

## Description  
Functions and utilities for the purpose of decoding, storing, processing, and visualizing AIS data. 


## What is AIS?
Wikipedia: https://en.wikipedia.org/wiki/Automatic_identification_system  
Description of protocol and AIS message types: https://gpsd.gitlab.io/gpsd/AIVDM.html  


## Install

The package can be installed using pip:
  ```
  python3 -m pip install git+https://gitlab.meridian.cs.dal.ca/matt_s/ais_public#egg=ais
  ```

To enable experimental visualization features, QGIS must also be installed and included in the PYTHONPATH env variable


## Configuring

A config file can be used to specify storage location for the database as well as directory paths for where to look for additional data.
The package will look for configs in the file `$HOME/.config/ais.cfg`, where $HOME is the user's home directory.
If no config file is found, the following defaults will be used
```
dbpath = $HOME/ais.db
data_dir = $HOME/ais/             
zones_dir = $HOME/ais/zones/
tmp_dir = $HOME/ais/tmp_parsing/
rawdata_dir = $HOME/ais/rawdata/
output_dir = $HOME/ais/scriptoutput/

host_addr = localhost
host_port = 9999
```

## Getting started: code examples

1. [Parsing raw format messages into a database](examples/example01_create_db_from_rawmsgs.py)

2. [Automatically generate SQL database queries](examples/example02_query_the_database.py)

3. Compute vessel trajectories  
  TODO: add documentation

4. Merging data from additional sources  
  TODO: add documentation

5. Scraping the web for vessel metadata  
  TODO: add documentation

6. [Compute network graph of vessel movements between polygons](examples/example04_network_graph.py)

7. Render visualizations  
  TODO: add documentation

## Collecting AIS Data

1. [Setting up an AIS radio station, and exchanging data with other networks](docs/AIS_base_station.md)



## How it Works

Greater detail on package functionality for developers, contributors, and AIS analysts

1. [Summary of package contents](docs/overview.md)

2. [SQL Database Schematics](docs/database.md)


Functions and utilities for the purpose of decoding, storing, processing, and visualizing AIS data. 

<img src="https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/tests/output/scriptoutput.png" alt="ais tracks - one month in the canadian atlantic" width="900"/>

## Install

The package can be installed using pip:
  ```
  python3 -m pip install git+https://gitlab.meridian.cs.dal.ca/matt_s/ais_public#egg=ais
  ```

To enable experimental visualization features, QGIS must also be installed and included in the PYTHONPATH env variable


## Configuring

A config file can be used to specify storage location for the database as well as directory paths for where to look for additional data.
The package will look for configs in the file `$HOME/.config/ais.cfg`, where $HOME is the user's home directory.
If no config file is found, the following defaults will be used:
```
dbpath = $HOME/ais.db
data_dir = $HOME/ais/             
zones_dir = $HOME/ais/zones/
tmp_dir = $HOME/ais/tmp_parsing/
rawdata_dir = $HOME/ais/rawdata/
```

## Getting started: code examples

#### [Parsing raw NMEA messages into a database](examples/example01_create_db_from_rawmsgs.py)

#### [Automatically generate SQL database queries](examples/example02_query_the_database.py)

#### Compute vessel trajectories
  TODO: add documentation

#### Merging data from additional sources
  TODO: add documentation

#### Scraping the web for vessel metadata
  TODO: add documentation

#### [Compute network graph of vessel movements between polygons](examples/example04_network_graph.py)

#### Render visualizations
  TODO: add documentation



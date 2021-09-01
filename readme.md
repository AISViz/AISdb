Functions and utilities for the purpose of decoding, storing, processing, and visualizing AIS data. 

<img src="https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/tests/output/scriptoutput.png" alt="ais tracks - one month in the canadian atlantic" width="900"/>

## Install

The package can be installed using pip:
  ```
  python3 -m pip install git+https://gitlab.meridian.cs.dal.ca/matt_s/ais_public#egg=ais
  ```

To enable experimental visualization features, QGIS must also be installed and included in the PYTHONPATH env variable


## Getting Started

### Parsing raw NMEA messages into a database


```
from ais import *

filepaths = os.listdir('/home/matt/ais_raw_NMEA/')    # filepaths to .nm4 message reports (list of strings)
dbpath = ais.dbpath                                   # location of where the database file will be stored
processes = 12                                        # number of processes to run in parallel. set to False to disable paralellizing

parallel_decode(filepaths, dbpath, processes)
```

Some points to note when decoding: 
  - Only position reports (messages 1, 2, 3, 18, 19) and static vessel reports (messages 5, 24) will be kept. All other messages are discarded.
  - Temporal resolution will be reduced to one message per MMSI per minute. The first occurring message will be kept.


### Querying the database  
  

##### Automatically generate SQL database queries
The qrygen() class can be used to generate SQL query code when given a set of input boundaries (in the form of a dictionary), as well as additional SQL filters. 
Some preset filter functions are included in ais/database/lambdas.py, however custom filter functions can be written as well.

```
from datetime import datetime 

qry_bounds = qrygen(
    start     = datetime(2021,1,1),
    end       = datetime(2021,1,2),
    mmsi      = 316002588,
  )

rows = qry_bounds.run_qry(
    dbpath    = dbpath, 
    qryfcn    = leftjoin_dynamic_static
    callback  = has_mmsi, 
  )
```
In this example, an SQL query will be created so search the database for all records of the vessel with MMSI identifier 316002588 between the date range of Jan 1, 2021 to Jan 2, 2021. 
The qryfcn 'leftjoin_dynamic_static' is the default query format to scan the database tables, which will merge both the static vessel message reports data as well as dynamic position reports.
By changing the callback function and qry_bounds parameters, different subsets of the data can be queried, for example, all vessels in the given bounding box within the specified date range.


##### Compute vessel trajectories
  TODO: add documentation


##### Compute network graph of vessel movements between polygons
  TODO: add documentation


## Configuring

A config file can be used to specify storage location for the database as well as directory paths for where to look for additional data.
The package will look for configs in the file `$HOME/.config/ais.cfg`, where $HOME is the user's home directory.
If no config file is found, the following defaults will be used:
```
dbpath = "$HOME/ais.db"
data_dir = "$HOME/ais/"             
zones_dir = "$HOME/ais/zones/"
```


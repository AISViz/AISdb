Functions and utilities for the purpose of decoding, storing, processing, and visualizing AIS data. 

<img src="https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/tests/output/scriptoutput.png" alt="ais tracks - one month in the canadian atlantic" width="600"/>

## Install

The package can be installed using pip:
  ```
  python3 -m pip install git+https://gitlab.meridian.cs.dal.ca/matt_s/ais_public#egg=ais
  ```

To enable experimental visualization features, QGIS must also be installed and included in the PYTHONPATH env variable


## Getting Started

### Parsing raw NMEA messages into a database


```
import os
import ais
from ais.database import parallel_decode

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
  TODO: add documentation  


##### Compute vessel trajectories
  TODO: add documentation


##### Compute network graph of vessel movements between polygons
  TODO: add documentation


![ais tracks - one month in the canadian atlantic](https://gitlab.meridian.cs.dal.ca/matt_s/ais_public/-/raw/master/tests/output/scriptoutput.png)


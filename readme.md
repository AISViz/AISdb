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
from ais import dbpath, decode_msgs

filepaths = ['/home/matt/ais_202101.nm4', '/home/matt/ais_202102.nm4', '.../etc']   # filepaths to raw AIS message data

decode_msgs(filepaths, dbpath)
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
from ais import dbpath, qrygen, leftjoin_dynamic_static, rtree_in_timerange_hasmmsi

#qry_bounds = qrygen(
#    start     = datetime(2021,1,1),
#    end       = datetime(2021,1,2),
#    mmsi      = 316023823,
#  )

qry_bounds = qrygen(
    start     = datetime(2019,10,1),
    end       = datetime(2019,10,31),
    mmsi      = 316023823.0,
  )

rows = qry_bounds.run_qry(
    dbpath    = dbpath, 
    qryfcn    = leftjoin_dynamic_static,
    callback  = rtree_in_timerange_hasmmsi, 
    #callback  = has_mmsi, 
  )

```
In this example, an SQL query will be created so search the database for all records of the vessel with MMSI identifier 316002588 between the date range of Jan 1, 2021 to Jan 2, 2021. 
The qryfcn 'leftjoin_dynamic_static' is the default query format to scan the database tables, which will merge both the static vessel message reports data as well as dynamic position reports.
By changing the callback function and qry_bounds parameters, different subsets of the data can be queried, for example, all vessels in the given bounding box within the specified date range.  

The resulting SQL code for this example is as follows:
```
WITH dynamic_202101 AS ( 
    SELECT CAST(m123.mmsi0 AS INT) as mmsi, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m123.msgtype
      FROM rtree_202101_msg_1_2_3 AS m123
      WHERE m123.mmsi0 = 316002588
    UNION
    SELECT CAST(m18.mmsi0 AS INT) as mmsi, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m18.msgtype
      FROM rtree_202101_msg_18 AS m18
      WHERE m18.mmsi0 = 316002588  
),
static_202101 AS ( 
    SELECT mmsi, vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, imo FROM static_202101_aggregate  
)
SELECT dynamic_202101.mmsi, dynamic_202101.t0, 
        dynamic_202101.x0, dynamic_202101.y0, 
        dynamic_202101.cog, dynamic_202101.sog, 
        dynamic_202101.msgtype, 
        static_202101.imo, static_202101.vessel_name,
        static_202101.dim_bow, static_202101.dim_stern, 
        static_202101.dim_port, static_202101.dim_star,
        static_202101.ship_type, ref.coarse_type_txt 
    FROM dynamic_202101 
LEFT JOIN static_202101
    ON dynamic_202101.mmsi = static_202101.mmsi
LEFT JOIN coarsetype_ref AS ref 
    ON (static_202101.ship_type = ref.coarse_type) 
ORDER BY 1, 2

```

The results of this automatically generated query will then be stored in the `rows` variable.


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
tmp_dir = "$HOME/ais/tmp_parsing/"
```


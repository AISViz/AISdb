from datetime import datetime 

from ais import dbpath, qrygen
from ais.database.qryfcn import leftjoin_dynamic_static
from ais.database.lambdas import rtree_in_time_bbox_validmmsi


'''
The qrygen() class can be used to generate SQL query code when given a set of input boundaries (in the form of a dictionary), as well as additional SQL filters. 
Some preset filter functions are included in ais/database/lambdas.py, however custom filter functions can be written as well.
'''


qry_bounds = qrygen(
    start     = datetime(2021,1,10),
    end       = datetime(2021,1,11),
    ymin      = 43.35715610154772, 
    xmin      = -69.50402957994562,
    ymax      = 52.01203702301506, 
    xmax      = -55.172026729758834,
  )

# to return all rows as an array
rows = qry_bounds.run_qry(
    dbpath    = dbpath, 
    qryfcn    = leftjoin_dynamic_static,
    callback  = rtree_in_time_bbox_validmmsi, 
  )

# or alternatively, create a row generator yielding arrays of rows per unique MMSI
rowgen = qry_bounds.gen_qry(
    dbpath    = dbpath, 
    qryfcn    = leftjoin_dynamic_static,
    callback  = rtree_in_time_bbox_validmmsi, 
  )
rows = next(rowgen)

'''
In this example, SQL code will be generated to search for all vessels in the approximate area of the Gulf of St Lawrence between 2021-01-10 and 2021-01-11.
The qryfcn 'leftjoin_dynamic_static' is the default query format to scan the database tables, which will merge both the static vessel message reports data as well as dynamic position reports.
By changing the callback function and qry_bounds parameters, different subsets of the data can be queried, for example, matching only vessels with a given MMSI identifier.  

The resulting SQL code for this example is as follows:

```
WITH dynamic_202101 AS (
    SELECT CAST(m123.mmsi0 AS INT) as mmsi, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m123.msgtype
      FROM rtree_202101_msg_1_2_3 AS m123
      WHERE
        m123.t0 >= 11059200 AND
        m123.t1 <= 11060640 AND
        m123.x0 >= -69.50402957994562 AND
        m123.x1 <= -55.172026729758834 AND
        m123.y0 >= 43.35715610154772 AND
        m123.y1 <= 52.01203702301506  AND
        m123.mmsi0 >= 201000000 AND
        m123.mmsi1 < 776000000
    UNION
    SELECT CAST(m18.mmsi0 AS INT) as mmsi, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m18.msgtype
      FROM rtree_202101_msg_18 AS m18
      WHERE
        m18.t0 >= 11059200 AND
        m18.t1 <= 11060640 AND
        m18.x0 >= -69.50402957994562 AND
        m18.x1 <= -55.172026729758834 AND
        m18.y0 >= 43.35715610154772 AND
        m18.y1 <= 52.01203702301506  AND
        m18.mmsi0 >= 201000000 AND
        m18.mmsi1 < 776000000
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

And the results of the query, containing columns:
mmsi, epoch_minutes, longitude, latitude, cog, sog, msgtype, IMO, vessel_name, bow_length, stern_length, portside_length, starboardside_length, ship_type, ship_type_text

```
>>> len(rows)
313750

>>> rows[0:10]
array([[209008000, 10400023.0, -58.12221908569336, 46.33310317993164,
        311.70000000000005, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13,
        10, 70, 'Cargo ships'],
       [209008000, 10400026.0, -58.13196563720703, 46.3388557434082,
        310.40000000000003, 12.1, 1, 9415222, 'LABRADOR', 156, 30, 13,
        10, 70, 'Cargo ships'],
       [209008000, 10400027.0, -58.13508605957031, 46.340736389160156,
        311.20000000000005, 12.1, 1, 9415222, 'LABRADOR', 156, 30, 13,
        10, 70, 'Cargo ships'],
       [209008000, 10400028.0, -58.13929748535156, 46.34328079223633,
        310.6, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13, 10, 70,
        'Cargo ships'],
       [209008000, 10400029.0, -58.143836975097656, 46.345787048339844,
        307.3, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13, 10, 70,
        'Cargo ships'],
       [209008000, 10400030.0, -58.147064208984375, 46.34745407104492,
        307.20000000000005, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13,
        10, 70, 'Cargo ships'],
       [209008000, 10400031.0, -58.150150299072266, 46.349159240722656,
        308.70000000000005, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13,
        10, 70, 'Cargo ships'],
       [209008000, 10400032.0, -58.15460968017578, 46.35163116455078,
        308.20000000000005, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13,
        10, 70, 'Cargo ships'],
       [209008000, 10400033.0, -58.158470153808594, 46.3536376953125,
        306.7, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13, 10, 70,
        'Cargo ships'],
       [209008000, 10400034.0, -58.162811279296875, 46.355953216552734,
        308.0, 12.0, 1, 9415222, 'LABRADOR', 156, 30, 13, 10, 70,
        'Cargo ships']], dtype=object)
```
''' 

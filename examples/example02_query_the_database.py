from datetime import datetime 

from ais import dbpath, qrygen
from ais.database.qryfcn import crawl
from ais.database.lambdas import rtree_in_time_bbox_validmmsi


'''
The qrygen() class can be used to generate SQL query code when given a set of input boundaries (in the form of a dictionary), as well as additional SQL filters. 
Some preset filter functions are included in ais/database/lambdas.py, however custom filter functions can be written as well. In this example, rtree_in_time_bbox_validmmsi() is used to apply SQL filters at query time
'''


qry = qrygen(
    start     = datetime(2021,1,10),
    end       = datetime(2021,1,11),
    ymin      = 43.35715610154772, 
    xmin      = -69.50402957994562,
    ymax      = 52.01203702301506, 
    xmax      = -55.172026729758834,
    callback  = rtree_in_time_bbox_validmmsi, 
  )

# to view the SQL code that will be executed
print(crawl(**qry))

# to return all rows as an array
rows = qry.run_qry(fcn=crawl, dbpath=dbpath)

# or alternatively, create a row generator yielding arrays of rows per unique MMSI
rowgen = qry.gen_qry(fcn=crawl, dbpath=dbpath)
rows = next(rowgen)

'''
Here SQL code is generated to search for all vessels in the approximate area of the Gulf of St Lawrence between 2021-01-10 and 2021-01-11.
The qryfcn crawl() is the default query format to scan the database tables, which will merge both the static vessel message reports data as well as dynamic position reports and perform a union over monthly tables.
By changing the callback function and qry_bounds parameters, different subsets of the data can be queried, for example, matching only vessels with a given MMSI identifier.  

A custom query function or callback function can also be written in place of the defaults if different SQL behaviour is desired.

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

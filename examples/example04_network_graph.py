from datetime import datetime, timedelta

import numpy as np

from aisdb import (
        Domain,
        ZoneGeom,
        ZoneGeomFromTxt, 
        merge_layers, 
        qrygen, 
        zones_dir,
    )
from aisdb.gis import glob_shapetxts
from aisdb.network_graph import graph
from aisdb.database.qryfcn import leftjoin_dynamic_static
from aisdb.database.lambdas import (
        rtree_in_bbox_time, 
        rtree_in_bbox_time_validmmsi, 
    )



#zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in glob_shapetxts(zones_dir=zones_dir)]} 

zonegeoms = {
    'Zone1': ZoneGeom(name='Zone1', 
                      x=[-170.24, -170.24, -38.5, -38.5, -170.24], 
                      y=[29.0, 75.2, 75.2, 29.0, 29.0])}
domain = Domain(name='new_domain', geoms=zonegeoms, cache=False)

# query db for points in domain 
qry = qrygen(
        start   = datetime(2020,9,1),
        end     = datetime(2020,9,3),
        xmin    = domain.minX, 
        xmax    = domain.maxX, 
        ymin    = domain.minY, 
        ymax    = domain.maxY,
        callback=rtree_in_bbox_time,
    )

# view the SQL code to be executed
print(crawl(**qry))

rowgen = qry.gen_qry(dbpath=dbpath, fcn=leftjoin_dynamic_static)

merged = merge_layers(rowgen, dbpath)

# TODO: describe how to write custom filters
network_graph.graph(merged, domain, dbpath, parallel=12, apply_filter=False)
    

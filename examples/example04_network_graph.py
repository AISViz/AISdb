from datetime import datetime, timedelta

import numpy as np

from ais import (
        network_graph, 
        merge_layers, 
        qrygen, 
        leftjoin_dynamic_static,
        rtree_in_bbox_time, 
        rtree_in_bbox_time_validmmsi, 
        ZoneGeomFromTxt, 
        Domain,
    )


# zones_dir should be defined in ~/.config/ais.cfg


shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)

# query db for points in domain 
rowgen = qrygen(
        start   = datetime(2020,9,1),
        end     = start + timedelta(hours=24),
        #end     = datetime(2020,10,1),
        xmin    = domain.minX, 
        xmax    = domain.maxX, 
        ymin    = domain.minY, 
        ymax    = domain.maxY,
    ).gen_qry(dbpath, callback=rtree_in_bbox_time, qryfcn=leftjoin_dynamic_static)

merged = merge_layers(rowgen, dbpath)

# TODO: describe how to write custom filters
network_graph.graph(merged, domain, dbpath, parallel=12, apply_filter=False)
    

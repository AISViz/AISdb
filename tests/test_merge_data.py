from datetime import datetime, timedelta

import numpy as np
import shapely.wkt

from aisdb import *
#from database import *
#from shapely.geometry import Polygon, LineString, MultiPoint
#from gis import *
#from track_gen import *
#from network_graph import *


#shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
#zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
from tests.create_testing_data import zonegeoms_or_randompoly
zonegeoms = zonegeoms_or_randompoly()
domain = Domain('testdomain', zonegeoms, cache=False)


def test_output_mergerows_bathy_shoredist_vesselgeom():

    start   = datetime(2020,9,1)
    end     = datetime(2020,10,1)

    start   = datetime(2018,6,1)
    end     = datetime(2018,7,1)

    rowgen = qrygen(
            #xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            end     = end,
            xmin    = domain.minX, 
            xmax    = domain.maxX, 
            ymin    = domain.minY, 
            ymax    = domain.maxY,
        ).gen_qry(dbpath, callback=rtree_in_bbox, qryfcn=leftjoin_dynamic_static)

    tracks = (next(trackgen(r)) for r in rowgen)

    merged = merge_layers(rowgen, dbpath)

    return


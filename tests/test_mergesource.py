from datetime import datetime, timedelta
import numpy as np
#np.set_printoptions(precision=5, linewidth=80, formatter=dict(datetime=datetime, timedelta=timedelta), floatmode='maxprec', suppress=True)
import shapely.wkt

from database import *
from shapely.geometry import Polygon, LineString, MultiPoint
from gis import *
#from track_viz import *
from track_gen import *


dbpath = '/run/media/matt/My Passport/june2018-06_test3.db'
dbpath = '/run/media/matt/My Passport/201806_test_paralleldecode.db'



def test_parse_regions():
    zones_east = zones_from_txts('../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', 'east')
    zones_west = zones_from_txts('../scripts/dfo_project/WestCoast_EEZ_Zones_12_8', 'west')
    '''
    zones = zones_east
    '''


def test_output_allsource():

    # join rtree tables with aggregate position reports 
    start   = datetime(2018,6,1)
    end     = datetime(2018,7,1)

    # query zones
    #aisdb = dbconn(dbpath)
    #conn, cur = aisdb.conn, aisdb.cur
    #cur.execute('SELECT objname, binary FROM rtree_polygons WHERE domain = "east"')
    #zones = dict(domain='east', geoms={p[0]: pickle.loads(p[1]) for p in cur.fetchall()})

    from shapely.ops import unary_union
    hull = unary_union(zones['geoms'].values()).convex_hull
    hull_xy = merge(zones['hull'].boundary.coords.xy)
    west, east, south, north = np.min(hull_xy[::2]), np.max(hull_xy[::2]), np.min(hull_xy[1::2]), np.max(hull_xy[1::2])

    # query db for points in domain convex hull
    dt = datetime.now()
    rows = qrygen(
            #xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            end     = end,
            xmin    = west, 
            xmax    = east, 
            ymin    = south, 
            ymax    = north,
        ).run_qry(dbpath, callback=rtree_in_bbox, qryfcn=leftjoin_dynamic_static)
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

    from dbclient import *
    merge_layers(rows, zones, dbpath)





    from importlib import reload
    import track_gen
    reload(track_gen)
    from track_gen import *

    tracks = np.array(list(trackgen(merged, colnames=colnames)))
    filters = [
            lambda track, rng: compute_knots(track, rng) < 50,
        ]
    
    # step into loops
    track = tracks[0]
    rng = list(segment(track, maxdelta=timedelta(hours=2), minsize=3))[0]
    mask = filtermask(track, rng, filters)
    n = sum(mask)
    c = 0
    chunksize=5000

    from shore_dist import shore_dist_gfw
    from gebco import Gebco
    from webdata import marinetraffic
    from wsa import wsa
    sdist = shore_dist_gfw()
    bathymetry = Gebco()
    hullgeom = marinetraffic.scrape_tonnage()

    sdist.__enter__()
    bathymetry.__enter__()
    hullgeom.__enter__()
    sdist.__exit__(None, None, None)
    bathymetry.__exit__(None, None, None)
    hullgeom.__exit__(None, None, None)


    




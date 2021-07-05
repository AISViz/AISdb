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
    end     = datetime(2018,6,2)

    dt = datetime.now()
    rows = qrygen(start=start, end=end).run_qry(dbpath, callback=rtree_in_time_mmsi, qryfcn=leftjoin_dynamic_static)
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

    aisdb = dbconn(dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    cur.execute('SELECT objname, binary FROM rtree_polygons WHERE domain = "east"')
    zones = dict(domain='east', geoms={p[0]: pickle.loads(p[1]) for p in cur.fetchall()})

    from importlib import reload
    import track_gen
    reload(track_gen)
    from track_gen import *

    tracks = np.array(list(trackgen(rows, colnames=colnames)))
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


    




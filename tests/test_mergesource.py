from datetime import datetime, timedelta
import numpy as np
#np.set_printoptions(precision=5, linewidth=80, formatter=dict(datetime=datetime, timedelta=timedelta), floatmode='maxprec', suppress=True)
import shapely.wkt
import pickle

from database import *
from shapely.geometry import Polygon, LineString, MultiPoint
from gis import *
#from track_viz import *
from track_gen import *
from network_graph import *


dbpath = '/run/media/matt/My Passport/june2018-06-01_test.db'
dbpath = '/run/media/matt/My Passport/june2018-06_test3.db'
dbpath = '/run/media/matt/My Passport/201806_test_paralleldecode.db'
dbpath = '/meridian/aisdb/eE_202009_test.db'
dbpath = '/run/media/matt/My Passport/eE_202009_test.db'


zones_east = zones_from_txts('../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', 'east')
zones_west = zones_from_txts('../scripts/dfo_project/WestCoast_EEZ_Zones_12_8', 'west')
zones = zones_east


#def test_parse_regions():
    #zones_east = zones_from_txts('../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', 'east')
    #zones_west = zones_from_txts('../scripts/dfo_project/WestCoast_EEZ_Zones_12_8', 'west')


def test_output_allsource():

    start   = datetime(2020,9,1)
    end     = datetime(2020,10,1)

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

    # query db for points in domain 
    west, east, south, north = np.min(hull_xy[::2]), np.max(hull_xy[::2]), np.min(hull_xy[1::2]), np.max(hull_xy[1::2])

    rowgen = qrygen(
            #xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            end     = end,
            xmin    = west, 
            xmax    = east, 
            ymin    = south, 
            ymax    = north,
        ).gen_qry(dbpath, callback=rtree_in_bbox, qryfcn=leftjoin_dynamic_static)

    tracks = (next(trackgen(r)) for r in rowgen)

    merged = merge_layers(rowgen, zones, dbpath)

    #graph(merged, zones, dbpath, parallel=True)

    return


def test_network_graph():

    start   = datetime(2018,6,1)
    end     = datetime(2018,7,1)

    # query zones
    '''
    aisdb = dbconn(dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    cur.execute('SELECT objname, binary FROM rtree_polygons WHERE domain = "east"')
    zones = dict(domain='east', geoms={p[0]: pickle.loads(p[1]) for p in cur.fetchall()})
    '''

    from shapely.ops import unary_union
    hull = unary_union(zones['geoms'].values()).convex_hull
    hull_xy = merge(zones['hull'].boundary.coords.xy)

    # query db for points in domain 
    west, east, south, north = np.min(hull_xy[::2]), np.max(hull_xy[::2]), np.min(hull_xy[1::2]), np.max(hull_xy[1::2])

    rowgen = qrygen(
            #xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            #end     = end,
            end     = start + timedelta(hours=24),
            xmin    = west, 
            xmax    = east, 
            ymin    = south, 
            ymax    = north,
        ).gen_qry(dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=leftjoin_dynamic_static)

    tracks = (next(trackgen(r)) for r in rowgen)

    merged = merge_layers(rowgen, zones, dbpath)

    graph(merged, zones, dbpath, parallel=12)
    

    ''' step-through
        

        colnames = [
                'mmsi', 'time', 'lon', 'lat', 
                'cog', 'sog', 'msgtype',
                'imo', 'vessel_name',  
                'dim_bow', 'dim_stern', 'dim_port', 'dim_star',
                'ship_type', 'ship_type_txt',
                'deadweight_tonnage', 'submerged_hull_m^2',
                'km_from_shore', 'depth_metres',
            ]

        track_merged = next(trackgen(next(merged), colnames))
        track_merged = next(trackgen(merged[4], colnames))

    '''


if False:  # testing

    with open('output/testrows', 'wb') as f:
        #assert len(rows) > 1000
        for row in rowgen:
            pickle.dump(row, f)

    with open('output/mergedrows', 'wb') as f:
        #assert len(list(merged)) > 1000
        #pickle.dump(merged, f)
        for row in merged:
            pickle.dump(row, f)
        
    rowgen = []
    with open('output/testrows', 'rb') as f:
        while True:
            try:
                rows = pickle.load(f)
            except EOFError as e:
                break
            rowgen.append(rows)

    merged = []
    with open('output/mergedrows', 'rb') as f:
        while True:
            try:
                rows = pickle.load(f)
            except EOFError as e:
                break
            merged.append(rows)

    colnames = [
            'mmsi', 'time', 'lon', 'lat', 
            'cog', 'sog', 'msgtype',
            'imo', 'vessel_name',  
            'dim_bow', 'dim_stern', 'dim_port', 'dim_star',
            'ship_type', 'ship_type_txt',
            'deadweight_tonnage', 'submerged_hull_m^2',
            'km_from_shore', 'depth_metres',
        ]


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

    gen = trackgen(rows, colnames=colnames[0:rows.shape[1]])
    for track in gen:

    gen = trackgen(merged, colnames=colnames[0:merged.shape[1]])
    for track in gen:
        if track['mmsi'] == 246770976: break

    rng = list(segment(track, maxdelta=timedelta(hours=3), minsize=1))[0]
    mask = filtermask(track, rng, filters)
    n = sum(mask)
    c = 0
    chunksize=500000

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


"""
db query 1 month: 41 min

script start:       3:40pm
processing start:   4:21pm
processing end:     crash 5:30pm  on mmsi 316008896

for mmsirows in merged:
    if mmsirows[0][0] == 316008896: 
        track = next(trackgen(mmsirows, colnames=colnames))
        break

rng = list(segment(track, maxdelta=timedelta(hours=3), minsize=1))[2]


"""

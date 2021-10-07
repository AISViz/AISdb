from datetime import datetime, timedelta

os.system("taskset -p 0xfff %d" % os.getpid())
from multiprocessing import set_start_method
set_start_method('forkserver')

import numpy as np
import shapely.wkt
from shapely.geometry import Polygon, LineString, MultiPoint
import pickle

from common import *
from network_graph import *
#from clustering import *
from database import *
from gis import *
from track_gen import *



shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)
# TODO: hashmap lookup for existing geoms ?? // database integration with Domain class


start = datetime(2021, 1, 1)
end = datetime(2021, 1, 14)

start = datetime(2019,9,1)
end = datetime(2019,10,1)


def test_network_graph():

    # query db for points in domain 
    rowgen = qrygen(
            start   = start,
            end     = end,
            #end     = start + timedelta(hours=24),
            xmin    = domain.minX, 
            xmax    = domain.maxX, 
            ymin    = domain.minY, 
            ymax    = domain.maxY,
        ).gen_qry(callback=rtree_in_bbox_time, qryfcn=leftjoin_dynamic_static)

    merged = merge_layers(rowgen)


    with open('tests/output/clustertest', 'rb') as f:
        merged = pickle.load(f)

    filters = [
            #lambda rowdict: rowdict['velocity_knots_max'] == 'NULL' or float(rowdict['velocity_knots_max']) > 50,
            lambda rowdict: rowdict['src_zone'] == 'Z0' and rowdict['rcv_zone'] == 'NULL',
            lambda rowdict: rowdict['minutes_spent_in_zone'] == 'NULL' or rowdict['minutes_spent_in_zone'] <= 1,
        ]

    #with import_handler() as importconfigs:
    graph(merged, domain, parallel=12, filters=filters)
    

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
        kwargs = dict(filters=filters)
        kwargs = {}
        parallel=12

        track_merged = next(trackgen(merged, colnames))

        track_merged = next(trackgen([merged[0]], colnames))


        track_merged = next(trackgen(merged, colnames))
        while track_merged['mmsi'] < 262006976:
            track_merged = next(trackgen(merged, colnames))


        aisdb.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        aisdb.cur.fetchall()

        aisdb.cur.execute("SELECT * from rtree_201910_msg_1_2_3 limit 10")
        aisdb.cur.execute("SELECT * from static_201910_aggregate limit 10")
        aisdb.cur.fetchall()

        for rows in merged:
            if len(np.unique(rows[:,1])) != len(rows[:,1]): break
        # track = next(trackgen(rows))

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
    with open('tests/output/testrows', 'rb') as f:
        while True:
            try:
                rows = pickle.load(f)
            except EOFError as e:
                break
            rowgen.append(rows)

    merged = []
    with open('tests/output/mergedrows', 'rb') as f:
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

    gen = trackgen([rows], colnames=colnames[0:rows.shape[1]])
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

maxdelta = timedelta(hours=1)

filters=[lambda track, rng: [True for _ in rng[:-1]]]

filters = [
    lambda track, rng: delta_knots(track, rng) < 50,
    lambda track, rng: delta_meters(track, rng) < 10000,
    lambda track, rng: delta_seconds(track, rng) > 0,
]

for mmsirows in merged:
    if mmsirows[0][0] == 316001408: 
        track_merged = next(trackgen(mmsirows, colnames=colnames))
        break

aggregator = agg_transits_per_segment(track_merged, filters)
transit_window, in_zones, zoneID = next(aggregator)

geofence(track_merged, domain, colnames, filters=None, maxdelta=timedelta(hours=1))

geofence_test2(track_merged, domain)

    backup_track_merged = track_merged.copy()

maxdelta = timedelta(hours=1)

mmsirows = next(merged)
track_merged = next(trackgen(mmsirows, colnames=colnames))
list(geofence(track_merged, domain, colnames, filters=filters))

"""

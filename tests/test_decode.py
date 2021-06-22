import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

import os

from database import *

#dbpath = '/run/media/matt/My Passport/ais_august.db'
#dbpath = 'output/test_decode.db'
dbpath = '/run/media/matt/My Passport/test_decode.db'
dbpath = '/run/media/matt/My Passport/june2018_vacuum.db'
fpath = '/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-06-01.csv'
#fpath = '/run/media/matt/My Passport/raw_test/exactEarth_historical_data_2021-04-01.nm4'

#os.remove(dbpath)
decode_raw_pyais(fpath, dbpath)




aisdb = dbconn(dbpath=dbpath)
conn, cur = aisdb.conn, aisdb.cur

cur.execute('select * from rtree_201806_msg_1_2_3 LIMIT 10')
#cur.execute('VACUUM INTO "/run/media/matt/My Passport/june2018_vacuum.db"')
cur.execute('select * from ais_201806_msg_1_2_3 LIMIT 10')
res = np.array(cur.fetchall())
res

from shapely.geometry import Polygon, LineString, MultiPoint
from gis import *
from track_viz import *
from track_gen import *

viz = TrackViz()

canvaspoly = viz.poly_from_coords()
#viz.add_feature_polyline(canvaspoly, ident='canvas_markers', opacity=0.35, color=(180, 230, 180))
canvaspoly = shapely.wkt.loads( 'POLYGON ((-61.51747881355931 46.25069648888631, -62.00013241525424 46.13520233725761, -62.19676906779659 45.77895246569407, -61.8452065677966 45.27803122330256, -61.56514830508475 45.10586058602501, -60.99907309322032 45.05537064981205, -60.71305614406779 45.20670660550304, -60.46875 45.56660601402942, -60.85010593220338 45.86615507310925, -61.13016419491525 45.92006919377324, -61.51747881355931 46.25069648888631))')
poly_xy = canvaspoly.boundary.coords.xy

qry = qrygen(
        xy = merge(canvaspoly.boundary.coords.xy),
        start   = datetime(2018,6,1),
        end     = datetime(2018,6,2),
        xmin    = min(poly_xy[0]), 
        xmax    = max(poly_xy[0]), 
        ymin    = min(poly_xy[1]), 
        ymax    = max(poly_xy[1]),
    ).crawl(callback=rtree_in_bbox_time_mmsi, qryfcn=rtree_minified) 

print(qry)
'''
qry=qry.replace('ais_', 'ais_s_')
cur.execute( 'EXPLAIN QUERY PLAN \n'  + qry)
'''

dt = datetime.now()
cur.execute(qry)
print(f'query time: {(datetime.now() - dt).seconds}s')

rows = np.array(cur.fetchall())



filters = [
        lambda track, rng: compute_knots(track, rng) < 40,
    ]

filters = [
        lambda track, rng: compute_knots(track, rng) < 1,
    ]

filters = [
        lambda track, rng: [True for _ in rng][:-1],
    ]

# generate track lines
identifiers = []
trackfeatures = []
ptfeatures = []
for track in trackgen(rows, ):#colnames=['mmsi', 'time', 'lon', 'lat', 'cog', 'sog']):
    rng = range(0, len(track['lon']))
    mask = filtermask(track, rng, filters)
    if track['lon'][rng][0] <= -180: mask[0] = False
    print(f'{track["mmsi"]} {rng=}:\tfiltered ', len(rng) - sum(mask),'/', len(rng))
    if sum(mask) < 2: continue
    linegeom = LineString(zip(track['lon'][rng][mask], track['lat'][rng][mask]))
    trackfeatures.append(linegeom)
    pts = MultiPoint(list(zip(track['lon'][rng][mask], track['lat'][rng][mask])))
    ptfeatures.append(pts)
    identifiers.append(track['type'][0] or track['mmsi'])


# pass geometry to application window
for ft, ident in zip(trackfeatures, identifiers): 
    viz.add_feature_polyline(ft, ident)

    '''
    i = 0

    i += 1
    ft = trackfeatures[i]
    ident=identifiers[i]
    viz.add_feature_polyline(ft, ident)

for track in trackgen(rows):#, colnames=colnames):
    #if track['mmsi'] == 316001312:
    if track['mmsi'] == 316002048:
        break

rows[rows[:,0] == 316002048]
    
    '''
    
viz.clear_lines()



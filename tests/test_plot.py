import os
from datetime import datetime, timedelta

import numpy as np
import shapely.wkt
from shapely.geometry import Polygon, LineString, MultiPoint


#import sys
#sys.path.append('/usr/share/qgis/python')


from database import *
from gis import *
from track_viz import TrackViz


shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)


def test_plot_matches_mmsi():

    viz = TrackViz()
    mmsi = 316001952

    start   = datetime(2018,6,1)
    end     = datetime(2018,7,1)

    rowgen = qrygen(
            #xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            #end     = end,
            end     = start + timedelta(hours=24),
            mmsi=mmsi,
            ).gen_qry(dbpath, 
                    callback=(lambda **kwargs: f'{rtree_in_timerange(**kwargs)} AND {rtree_has_mmsi(**kwargs)}'), qryfcn=leftjoin_dynamic_static)

    rows = next(rowgen)

    filters = [lambda track, rng: [True for _ in rng][:-1], ]

    # generate track lines
    identifiers = []
    trackfeatures = []
    ptfeatures = []
    for track in trackgen(rows):
        rng = range(0, len(track['lon']))
        mask = filtermask(track, rng, filters=filters, first_val=track['lon'][rng][0] > -180)
        print(f'{track["mmsi"]} {rng=}:\tfiltered ', len(rng) - sum(mask),'/', len(rng))
        if sum(mask) < 2: continue
        linegeom = LineString(zip(track['lon'][rng][mask], track['lat'][rng][mask]))
        trackfeatures.append(linegeom)
        pts = MultiPoint(list(zip(track['lon'][rng][mask], track['lat'][rng][mask])))
        ptfeatures.append(pts)
        #identifiers.append(track['type'] or track['mmsi'])
        identifiers.append(track['mmsi'])

    for ft, ident in zip(trackfeatures, identifiers): 
        viz.add_feature_polyline(ft, ident)

    viz.clear_lines()
    

def test_plot_domain():
    for txt, geom in list(domain.geoms.items()):
        viz.add_feature_polyline(geom.geometry, ident=txt, opacity=0.3, color=(200, 200, 200))

    viz.clear_polygons()


def test_plot_smallboundary():
    os.environ['QT_FATAL_WARNINGS'] = '1'


    #canvaspoly = viz.poly_from_coords()  # select map coordinates with the cursor
    canvaspoly = shapely.wkt.loads( 'POLYGON ((-61.51747881355931 46.25069648888631, -62.00013241525424 46.13520233725761, -62.19676906779659 45.77895246569407, -61.8452065677966 45.27803122330256, -61.56514830508475 45.10586058602501, -60.99907309322032 45.05537064981205, -60.71305614406779 45.20670660550304, -60.46875 45.56660601402942, -60.85010593220338 45.86615507310925, -61.13016419491525 45.92006919377324, -61.51747881355931 46.25069648888631))')
    poly_xy = canvaspoly.boundary.coords.xy

    #viz = TrackViz()
    QgsApplication.setPrefixPath('/usr', True)
    qgsApp = QgsApplication([], True)
    qgsApp.initQgis()


    viz = VizThread('testing', qgsApp)

    QgsApplication.taskManager().addTask(viz)
    #viz.run()

    canvaspoly = viz.poly_from_coords()
    poly_xy = canvaspoly.boundary.coords.xy

    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,2),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=leftjoin_dynamic_static) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

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
        #identifiers.append(track['type'] or track['mmsi'])
        identifiers.append(track['mmsi'])

    for ft, ident in zip(trackfeatures, identifiers): 
        viz.add_feature_polyline(ft, ident)

    viz.clear_lines()
    
    '''
    i = 0

    i += 1
    ft = trackfeatures[i]
    ident=identifiers[i]
    viz.add_feature_polyline(ft, ident)

    rows[rows[:,0] == 316002048]
    '''


def test_cluster_stopped():
    import hdbscan
    from shapely.geometry import Polygon, LineString, MultiPoint

    viz = TrackViz()

    canvaspoly = viz.poly_from_coords()
    poly_xy = canvaspoly.boundary.coords.xy

    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,7),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=leftjoin_dynamic_static) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

    filters = [
            lambda track, rng: compute_knots(track, rng) < 1,
        ]

    # generate track lines
    cluster_x = []
    cluster_y = []
    for track in trackgen(rows, ):#colnames=['mmsi', 'time', 'lon', 'lat', 'cog', 'sog']):
        rng = range(0, len(track['lon']))
        mask = filtermask(track, rng, filters)
        if track['lon'][rng][0] <= -180: mask[0] = False
        print(f'{track["mmsi"]} {rng=}:\tfiltered ', len(rng) - sum(mask),'/', len(rng))
        if sum(mask) <= 2: continue
        cluster_x = np.append(cluster_x, track['lon'][rng][mask])
        cluster_y = np.append(cluster_y, track['lat'][rng][mask])

    pos_matrix = np.vstack((cluster_x, cluster_y)).T
    pos_matrix_rad = np.vstack(([np.deg2rad(x) for x in cluster_x], [np.deg2rad(y) for y in cluster_y])).T

    clusterer = hdbscan.HDBSCAN(
            min_cluster_size=5, 
            metric='haversine', 
            prediction_data=False, 
            cluster_selection_epsilon=0.003)

    labels = clusterer.fit_predict(pos_matrix_rad)

    for label in np.unique(labels):
        if label == -1: continue
        cluster_idx = labels == label
        if len(np.unique(pos_matrix[cluster_idx][:,0])) < 2 or len(np.unique(pos_matrix[cluster_idx][:,1])) < 2: continue
        pts = MultiPoint( pos_matrix[cluster_idx] ) 
        #viz.add_feature_point(pts, ident=label)
        polyxy = merge(unary_union(pts).convex_hull.boundary.coords.xy)
        poly = Polygon(zip(polyxy[::2], polyxy[1::2]))
        viz.add_feature_polyline(poly, label)

    viz.clearfeatures()
    

exit()

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
    #identifiers.append(track['type'] or track['mmsi'])
    identifiers.append(track['mmsi'])


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


'''
https://en.wikipedia.org/wiki/Image_segmentation#Clustering_methods
https://geoffboeing.com/2014/08/clustering-to-reduce-spatial-data-set-size/
'''


import pickle

from shapely.geometry import MultiPoint, Point, LineString

from track_viz import TrackViz
from database.lambdas import *
from database.qryfcn import leftjoin_dynamic_static
from merge_data import merge_layers
from clustering import * 

assert track_dicts


shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)

viz = TrackViz()

for txt, geom in list(domain.geoms.items()):
    viz.add_feature_polyline(geom.geometry, ident=txt, opacity=0.3, color=(200, 200, 200))



test_cluster_duplicate_mmsi(track_dicts):

    rowgen = qrygen(
            start   = datetime(2019,9,1),
            end     = datetime(2019,9,8),
            #end     = start + timedelta(hours=24),
            xmin    = domain.minX, 
            xmax    = domain.maxX, 
            ymin    = domain.minY, 
            ymax    = domain.maxY,
        ).gen_qry(callback=rtree_in_bbox_time, qryfcn=leftjoin_dynamic_static)

    track_dicts = list(merge_layers(rowgen))

    with open('tests/output/clustertest', 'wb') as f:
        assert track_dicts != []
        pickle.dump(track_dicts, f)

    with open('tests/output/clustertest', 'rb') as f:
        track_dicts = pickle.load(f)
    merged=track_dicts



    # count vessels exceeding 50 knots
    n = 0
    for track in track_dicts:
        if flag(track):
            n += 1
            print(f'flagged {track["mmsi"]}\ttotal flagged: {n}')
    print(f'\nflagged {n}/{len(track_dicts)}\t( {n/len(track_dicts) * 100:.2f}% )')
    


    # example bad track
    # 311000128 example of too many tracks being clustered
    # 316002176 some values near halifax are false negatives
    tracks = iter(track_dicts)
    while (track := next(tracks))['mmsi'] != 311000128: 
        pass

        
    # manually iterate through flagged tracks for testing
    tracks = iter(track_dicts)

    while flag(track := next(tracks)) == False:
        pass
    print(track['mmsi'])

    # test the clustering for this track
    linegeom = LineString(zip(track['lon'], track['lat']))
    viz.add_feature_polyline(linegeom, ident=track['mmsi'], color=(255,0,0,128))
    for cluster in segment_tracks_dbscan(segment_tracks_timesplits([track]), max_cluster_dist_km=50):
        if len(cluster['time']) == 1: continue
        ptgeom = MultiPoint([Point(x,y) for x,y in zip(cluster['lon'], cluster['lat'])])
        viz.add_feature_point(ptgeom, ident=cluster['cluster_label']*100 if cluster['cluster_label']!=None else None)
        linegeom = LineString(zip(cluster['lon'], cluster['lat']))
        viz.add_feature_polyline(linegeom, ident=cluster['mmsi'], color=(30,30,255,255))

    viz.clear_points()
    viz.clear_lines()

    viz.clearfeatures()


    viz = TrackViz()

    

import pickle

from shapely.geometry import MultiPoint, Point, LineString

from aisdb.common import zones_dir
from qgis_window import ApplicationWindow
from database.lambdas import *
from database.qryfcn import leftjoin_dynamic_static
from webdata.merge_data import merge_layers
from track_gen import segment_tracks_timesplits
from gis import Domain, ZoneGeom, ZoneGeomFromTxt
#from clustering import * 

#assert track_dicts


shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
domain = Domain('east', zonegeoms)

viz = ApplicationWindow()

for txt, geom in list(domain.geoms.items()):
    viz.add_feature_polyline(geom.geometry, ident=txt, opacity=0.3, color=(200, 200, 200))



def test_cluster_duplicate_mmsi(track_dicts):

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

    #with open('tests/output/clustertest', 'wb') as f:
    #    assert track_dicts != []
    #    pickle.dump(track_dicts, f)

    fpath = os.path.join(output_dir, 'rowgen_year_test2.pickle')
    distsplit = partial(segment_tracks_encode_greatcircledistance, distance_meters=125000)
    merged = distsplit(split_len(timesplit(trackgen(deserialize_generator(fpath)))))

    #with open('tests/output/clustertest', 'rb') as f:
    #    track_dicts = pickle.load(f)
    #merged=track_dicts



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

    viz.clear_points()
    viz.clear_lines()

    while flag(track := next(tracks)) == False:
        pass
    print(track['mmsi'])


    # test the clustering for this track
    viz.clear_lines()
    n = 0
    showpts = False
    #linegeom = LineString(zip(track['lon'], track['lat']))
    #viz.add_feature_polyline(linegeom, ident=track['mmsi'], color=(255,0,0,128))
    for cluster in tracks:
        if len(cluster['time']) == 1: continue
        print(cluster['mmsi'], end='\r')
        if showpts:
            ptgeom = MultiPoint([Point(x,y) for x,y in zip(cluster['lon'], cluster['lat'])])
            viz.add_feature_point(ptgeom, ident=str((n+100)*100))
        linegeom = LineString(zip(cluster['lon'], cluster['lat']))
        viz.add_feature_polyline(linegeom, ident=cluster['mmsi'])  #cluster['mmsi']#, #color=(30,30,255,255))
        n += 1
        if n > 1000: break


    viz.clearfeatures()

    viz.clear_lines()
    viz.clear_points()


    viz = ApplicationWindow()

    

import os
from functools import partial 
from datetime import timedelta
from functools import reduce
import json

import shapely
from shapely.geometry import Point, LineString, Polygon, MultiPoint
import numpy as np
import requests

from aisdb import zones_dir, output_dir
from aisdb.gis import Domain, ZoneGeomFromTxt, glob_shapetxts
#from aisdb.track_gen import trackgen, segment_tracks_timesplits, fence_tracks, max_tracklength, segment_tracks_encode_greatcircledistance, concat_realisticspeed, mmsifilter, mmsirange
#from aisdb.network_graph import serialize_network_edge
#from aisdb.proc_util import deserialize_generator
from aisdb.qgis_window import ApplicationWindow

# Available platform plugins are: eglfs, linuxfb, minimal, minimalegl, offscreen, vnc, xcb
#os.environ['QT_DEBUG_PLUGINS'] = '1'
#os.environ['QT_QPA_PLATFORM'] = 'linuxfb'
#os.environ['QT_PLUGIN_PATH'] = '/usr/lib/qt/plugins'
#os.environ['XDG_RUNTIME_DIR'] = '/tmp/runtime-ais_env'


def test_plot_pointfeature():
    viz = ApplicationWindow()
    req = requests.get('https://ipinfo.io/json')
    assert req.ok
    loc = json.loads(req.content)['loc']
    y, x = map(float, loc.split(','))
    #geom = shapely.geometry.MultiPoint([(x,y)])
    geom = shapely.geometry.Point(x,y)
    viz.add_feature_point(geom, ident=loc)
    viz.exit()



def test_plot_polygon_geometry():
    keyorder = lambda key: int(key.rsplit(os.path.sep, 1)[1].split('.')[0].split('Z')[1])
    shapefilepaths = glob_shapetxts(keyorder=keyorder)

    zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
    #domain = Domain('west', {k:v for k,v in zonegeoms.items() if 'WZ' in k}, clearcache=True)
    domain = Domain('east', {k:v for k,v in zonegeoms.items() if 'EZ' in k})

    for txt, geom in list(domain.geoms.items()):
        viz.add_feature_poly(geom.geometry, ident=txt, opacity=0.3, color=(200, 200, 200))


def test_plot_processed_track():
    fpath = os.path.join(output_dir, 'rowgen_year_test2.pickle')

    timesplit = partial(segment_tracks_timesplits,  maxdelta=timedelta(hours=24))
    distsplit = partial(segment_tracks_encode_greatcircledistance, maxdistance=100000, cuttime=timedelta(hours=24), cutknots=50, minscore=0.000005) 
    geofenced = partial(fence_tracks,               domain=domain)
    serialize = partial(serialize_network_edge,     domain=domain)


    viz.clear_lines()
    viz.clear_points()
    #testmmsi = [218158000, 316043424, 316015104]
    testmmsi = [218158000, 316001088, 316043424, 316015104]
    rowgen = deserialize_generator(fpath)
    #tracks = max_tracklength(trackgen(mmsifilter(rowgen, mmsis=testmmsi)))
    tracks = distsplit(timesplit(trackgen(mmsifilter(rowgen, mmsis=testmmsi))))
    #tracks = distsplit(trackgen(mmsifilter(rowgen, mmsis=testmmsi)))


    n = 0
    for track in tracks:
        if len(track['time']) < 1:
            assert False
        elif len(track['time']) == 1:
            ptgeom = MultiPoint([Point(x,y) for x,y in zip(track['lon'], track['lat'])])
            viz.add_feature_point(ptgeom, ident=track['mmsi']+1000)
        else:
            linegeom = LineString(zip(track['lon'], track['lat']))
            viz.add_feature_line(linegeom, ident=track['mmsi']+1000)
        n += 1
        print(f'{n}')
    viz.focus_canvas_item(domain=domain)

    viz.render_vectors(fname='test4.png')


if False:

    from aisdb.track_viz import processing
    processing.algorithmHelp('qgis:union')

    params = {
            #'INPUT': self.basemap_lyr,
            'INPUT': vl3,
            'OVERLAY': vl3,
            'OUTPUT': os.path.join(output_dir, 'testimg.png'),
        }
    feedback = QgsProcessingFeedback()

    res = processing.run('qgis:union', params, feedback=feedback)

    from qgis.core import QgsLabelPosition, QgsProcessingFeedback


    '''
    notable changes:
        split on speed>50knots, rejoined using distance+time score
            - has weird effects at boundary of domain, probably doesnt affect zone crossings though
        in cases where scores tie, last one is picked instead of first one
        changed score datatype to 16-bit float (lower precision means more ties, causing it to prefer more recent tracks)
        corrected indexing error when computing scores
        took the average of 2 nearest scores instead of single score
        
        tested minimum score 
    '''


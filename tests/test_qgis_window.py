import os
from functools import partial
from datetime import datetime, timedelta
import json

import shapely
from shapely.geometry import Point, LineString, MultiPoint
import requests

from aisdb import output_dir
from aisdb.database.dbqry import DBQuery
from aisdb.database.sqlfcn_callbacks import in_validmmsi_bbox
from aisdb.gis import Domain
from aisdb.qgis_window import ApplicationWindow
from aisdb.track_gen import TrackGen, segment_tracks_encode_greatcircledistance
from tests.create_testing_data import zonegeoms_or_randompoly

# Available platform plugins are: eglfs, linuxfb, minimal, minimalegl,
#       offscreen, vnc, xcb
# os.environ['QT_DEBUG_PLUGINS'] = '1'
# os.environ['QT_QPA_PLATFORM'] = 'linuxfb'
# os.environ['QT_PLUGIN_PATH'] = '/usr/lib/qt/plugins'
# os.environ['XDG_RUNTIME_DIR'] = '/tmp/runtime-ais_env'
start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)


def plot_pointfeature(viz):
    #viz = ApplicationWindow()
    req = requests.get('https://ipinfo.io/json')
    assert req.ok
    loc = json.loads(req.content)['loc']
    y, x = map(float, loc.split(','))
    geom = shapely.geometry.Point(x, y)
    viz.add_feature_point(geom, ident=loc)
    #viz.exit()


def plot_polygon_geometry(viz):

    zonegeoms = zonegeoms_or_randompoly(randomize=True)
    domain = Domain('testing!', {k: v
                                 for k, v in zonegeoms.items()},
                    cache=False)
    for txt, geom in list(domain.geoms.items()):
        viz.add_feature_poly(geom.geometry,
                             ident=txt,
                             opacity=0.3,
                             color=(200, 200, 200))
    viz.focus_canvas_item(domain=domain)

    viz.clear_polygons()


def plot_processed_track(viz):
    zonegeoms = zonegeoms_or_randompoly(randomize=True, count=10)
    domain = Domain(name='test', geoms=zonegeoms, cache=False)

    # query db for points in domain bounding box
    args = DBQuery(
        start=start,
        end=end,
        xmin=domain.minX,
        xmax=domain.maxX,
        ymin=domain.minY,
        ymax=domain.maxY,
        callback=in_validmmsi_bbox,
    )
    rowgen = args.gen_qry()

    distsplit = partial(segment_tracks_encode_greatcircledistance,
                        maxdistance=250000,
                        cuttime=timedelta(weeks=1),
                        cutknots=45,
                        minscore=5e-07)
    tracks = distsplit(TrackGen(rowgen))
    #tracks = distsplit(TrackGen(mmsifilter(rowgen, mmsis=testmmsi)))

    n = 0
    for track in tracks:
        if len(track['time']) < 1:
            assert False
        elif len(track['time']) == 1:
            ptgeom = MultiPoint(
                [Point(x, y) for x, y in zip(track['lon'], track['lat'])])
            viz.add_feature_point(ptgeom, ident=track['mmsi'] + 1000)
        else:
            linegeom = LineString(zip(track['lon'], track['lat']))
            viz.add_feature_line(linegeom, ident=track['mmsi'] + 1000)
        n += 1
        print(f'{n}', end='|')
        if n > 250:
            break
    # viz.focus_canvas_item(domain=domain)

    viz.render_vectors(fname='test4.png')
    #viz.exit()


def test_run_QGIS_tests():
    '''
    run QGIS application tests

    the application can only be instantiated once or else QGIS will segfault
    hence, only one test is run using pytest, sharing the same application

    '''
    viz = ApplicationWindow()
    plot_pointfeature(viz)
    plot_polygon_geometry(viz)
    plot_processed_track(viz)
    input("press 'enter' to close QGIS window and continue")
    viz.exit()


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

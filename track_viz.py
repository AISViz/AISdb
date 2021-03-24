import os
from hashlib import sha256

import numpy as np
from qgis.core import *
from qgis.gui import QgsLayerTreeMapCanvasBridge, QgsMapCanvas #, QgsMapCanvasItem, QgsRubberBand
from qgis.PyQt.QtGui import QColor, QGuiApplication
import shapely.ops
import shapely.wkt
from shapely.geometry import LineString, Polygon

#from track_geom import *


# https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html


colorhash = lambda mmsi: f'#{sha256(str(mmsi).encode()).hexdigest()[-6:]}'
ring2qwkt = lambda ring: f'POLYGON({ring.wkt.split(" ", 1)[1]})'
#hexatriad = lambda hexa: (hexa, f'{hexa[0]}{hexa[-2:]}{hexa[1:-2]}', f'{hexa[0]}{hexa[3:]}{hexa[1:3]}',)


class TrackViz():
    def __init__(self):
        assert os.path.isfile('/usr/bin/qgis'), 'couldnt find qgis in path'
        QgsApplication.setPrefixPath('/usr', True)
        self.qgs = QgsApplication([], True)
        self.qgs.initQgis()
        self.canvas = QgsMapCanvas()
        self.bridge = QgsLayerTreeMapCanvasBridge(QgsProject.instance().layerTreeRoot(), self.canvas)
        self.project = QgsProject.instance()
        self.project.read('/home/matt/Desktop/qgis/ecoregion_test.qgz')
        #self.project.fileName()
        self.layout = self.project.instance().layoutManager().layoutByName('scripted_layout')
        self.layout.addItem(QgsLayoutItemMap(self.layout))
        self.canvas.show()
        # load map
        url = 'crs=EPSG:3857&format&tilePixelRatio=2&type=xyz&url=http://ecn.t3.tiles.virtualearth.net/tiles/a%7Bq%7D.jpeg?g%3D1&zmax=18&zmin=0'
        layer = QgsRasterLayer(url, 'Bing Maps', 'wms')
        assert layer.isValid(), 'error loading basemap'
        self.project.instance().addMapLayer(layer)

    def split_feature_over_meridian(self, meridian, geom):
        adjust = lambda x: ((np.array(x) + 180) % 360) - 180  
        if isinstance(geom, str): geom = shapely.wkt.loads(geom)
        merged = shapely.ops.linemerge([geom.boundary, meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        splits = [Polygon(zip(adjust((p := next(decomp)).boundary.coords.xy[0]), p.boundary.coords.xy[1])),
                  Polygon(zip(np.abs(adjust((p := next(decomp)).boundary.coords.xy[0])), p.boundary.coords.xy[1])) ]
        return splits

    def layer_from_feature(self, geomwkt, ident, color=None, opacity=None, nosplit=False):
        if nosplit == False and (meridian := LineString(np.array(((-180, -180, 180, 180), (-90, 90, 90, -90))).T)).crosses(shapely.wkt.loads(geomwkt)):
            print(f'splitting {ident}')
            splits = self.split_feature_over_meridian(meridian, geomwkt)
            return [self.layer_from_feature(splits[0].wkt, ident+'A', color, opacity, nosplit=True)[0],
                    #self.layer_from_feature(splits[1].wkt.replace('-', ' '), ident+'B', color, opacity, nosplit=True)[0]]
                    self.layer_from_feature(splits[1].wkt, ident+'B', color, opacity, nosplit=True)[0]]
        if color is None: color = colorhash(ident)
        geomtype = geomwkt.split('(', 1)[0].rstrip().upper()
        if opacity is None: opacity = 0.5 if geomtype in ('POLYGON','LINEARRING') else 1
        vl = QgsVectorLayer(geomtype, str(ident), 'memory')
        pr = vl.dataProvider()
        seg = QgsFeature()
        seg.setGeometry(QgsGeometry.fromWkt(geomwkt))
        pr.addFeatures([seg])
        vl.updateExtents()
        symbol = QgsSymbol.defaultSymbol(vl.geometryType())
        symbol.setColor(QColor(color))
        symbol.setOpacity(opacity)
        vl.renderer().setSymbol(symbol)
        return [vl]

    def update(self, layers): 
        self.project.instance().addMapLayers(layers)
        QGuiApplication.processEvents()

    def save(self, fname='map.png'):
        self.canvas.saveAsImage(os.path.join('output', fname))

    def clearfeatures(self, layerids):
        self.project.instance().removeMapLayers(layerids)
        self.canvas.refresh()

    def exit(self):
        self.project.write('scripts/ecoregion_test/testplot.qgz')
        self.canvas.close()
        self.qgs.closeAllWindows()
        self.qgs.exitQgis()


import os
from hashlib import sha256

import numpy as np
from PyQt5.QtWidgets import QApplication, QMainWindow, QLabel, QFrame, QStatusBar
from qgis.core import *
from qgis import QtCore
QSize = QtCore.QSize
from qgis.gui import QgsLayerTreeMapCanvasBridge, QgsMapCanvas #, QgsMapCanvasItem, QgsRubberBand
from qgis.PyQt.QtGui import QColor, QGuiApplication, QImage, QPainter, QgsStatusBar
import shapely.ops
import shapely.wkt
from shapely.geometry import LineString, Polygon

#from track_geom import *


# https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html


colorhash = lambda mmsi: f'#{sha256(str(mmsi).encode()).hexdigest()[-6:]}'
ring2qwkt = lambda ring: f'POLYGON({ring.wkt.split(" ", 1)[1]})'
#hexatriad = lambda hexa: (hexa, f'{hexa[0]}{hexa[-2:]}{hexa[1:-2]}', f'{hexa[0]}{hexa[3:]}{hexa[1:3]}',)


class TrackViz(QMainWindow):
    def __init__(self, projpath='/home/matt/Desktop/qgis/ecoregion_test.qgz'):
        ''' create track_viz application '''

        # initialize qt app
        assert os.path.isfile('/usr/bin/qgis'), 'couldnt find qgis in path'
        QgsApplication.setPrefixPath('/usr', True)
        self.qgs = QgsApplication([], True)
        super().__init__()
        self.qgs.initQgis()
        self.qgs.setApplicationName('ais_track_viz')
        self.qgs.setApplicationDisplayName('ais_track_viz')
        self.qgs.setMaxThreads(8)

        # create canvas window
        self.canvas = QgsMapCanvas()
        self.bridge = QgsLayerTreeMapCanvasBridge(QgsProject.instance().layerTreeRoot(), self.canvas)
        self.project = QgsProject.instance()

        # load basemap layer 
        url = 'crs=EPSG:3857&format&tilePixelRatio=2&type=xyz&url=http://ecn.t3.tiles.virtualearth.net/tiles/a%7Bq%7D.jpeg?g%3D1&zmax=18&zmin=0'
        layer = QgsRasterLayer(url, 'Bing Maps', 'wms')
        assert layer.isValid(), 'error loading basemap'
        self.project.instance().addMapLayer(layer)

        # configs
        self.settings = self.canvas.mapSettings()
        self.settings.setFlag(QgsMapSettings.Antialiasing, True)
        #self.settings.setOutputSize(QSize(1920, 1080))
        #self.settings.setFlag(QgsMapSettings.DrawLabeling, False)

        '''
        # testing
        #job = QgsMapRendererSequentialJob(self.settings)
        #self.settings.setoutputsize(QSize(960, 540))
        # https://gis.stackexchange.com/questions/153065/adding-qgis-feature-that-shows-coordinates-and-scale-into-a-custom-application
        self.showcoord = QLabel()
        self.showcoord.setFrameStyle(QFrame.Box)
        self.showcoord.setMinimumWidth(170)
        self.showcoord.setAlignment(QtCore.Qt.AlignCenter)
        self.statbar = QStatusBar()
        self.statbar.setSizeGripEnabled(False)
        self.statbar.addPermanentWidget(self.showcoord, 0)
        #self.scale = QLabel()
        #self.scale.setFrameStyle(QFrame.StyledPanel)
        #self.scale.setMinimumWidth(140)
        #self.statbar.addPermanentWidget(self.scale, 0)
        #self.connectNotify(self.canvas, )
        self.setStatusBar(self.statbar)
        '''

        # show the canvas 
        self.canvas.show()

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

    def update_features(self, layers): 
        self.project.instance().addMapLayers(layers)
        QGuiApplication.processEvents()

    def render_bg(self, layers, fname): 
        image = QImage(QSize(1920,1080), QImage.Format_RGB32)
        painter = QPainter(image)
        self.settings.setLayers(layers + self.settings.layers())
        job = QgsMapRendererCustomPainterJob(self.settings, painter)
        job.renderSynchronously()
        painter.end()
        image.save(f'output{os.path.sep}{fname}')

    def save(self, fname='map.png'):
        self.canvas.saveAsImage(os.path.join('output', fname))

    def clearfeatures(self, layerids):
        self.project.instance().removeMapLayers(layerids)
        self.canvas.refresh()

    def clearall(self):
        self.project.instance().removeMapLayers([lyr.id() for lyr in self.settings.layers()])
        self.canvas.refresh()

    def exit(self):
        self.project.write('scripts/ecoregion_test/testplot.qgz')
        self.canvas.close()
        self.qgs.closeAllWindows()
        self.qgs.exitQgis()

    def export_qgis(self, projpath):
        #self.project.fileName()
        assert projpath[-4:] == '.qgz', 'project path must end with .qgz'
        self.project.write(projpath)

    def import_qgis(self, projpath):
        self.project.read(projpath)
        #self.layout = self.project.instance().layoutManager().layoutByName('scripted_layout')
        #self.layout.addItem(QgsLayoutItemMap(self.layout))



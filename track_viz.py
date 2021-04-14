import os
import sys
from hashlib import sha256

#from PyQt5.QtWidgets import QWidget, QApplication, QMainWindow, QLabel, QFrame, QStatusBar, QVBoxLayout
#from qgis.core import *
from qgis.core import QgsApplication, QgsProject, QgsRasterLayer, QgsPrintLayout, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsMapSettings, QgsPointXY, QgsGeometry
from qgis import QtCore
from qgis.gui import QgsMapCanvas, QgsMapToolPan, QgsMapToolZoom, QgsRubberBand, QgsMapCanvasItem, QgsVertexMarker, QgsMapToolEmitPoint, QgsStatusBar
#QgsLayerTreeMapCanvasBridge
from qgis.PyQt.QtGui import QColor, QGuiApplication, QImage, QPainter, QPainterPath
from qgis.PyQt.QtWidgets import QMainWindow, QWidget, QLabel, QFrame, QStatusBar, QVBoxLayout, QApplication, QAction

import numpy as np
import shapely.ops
import shapely.wkt
from shapely.geometry import LineString, Polygon

#from track_geom import *


# https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html


colorhash = lambda mmsi: f'#{sha256(str(mmsi).encode()).hexdigest()[-6:]}'
ring2qwkt = lambda ring: f'POLYGON({ring.wkt.split(" ", 1)[1]})'
#hexatriad = lambda hexa: (hexa, f'{hexa[0]}{hexa[-2:]}{hexa[1:-2]}', f'{hexa[0]}{hexa[3:]}{hexa[1:3]}',)


class toolCoord(QgsMapToolEmitPoint):
    ''' custom map interface tool for retrieving coordinates at the cursor position
        reference: 
        https://github.com/NationalSecurityAgency/qgis-latlontools-plugin/blob/master/copyLatLonTool.py
    '''

    def __init__(self, canvas, statbar, project):
        self.canvas = canvas
        super().__init__(self.canvas)
        self.statbar = statbar
        crs_src = QgsCoordinateReferenceSystem("EPSG:3857")
        crs_dest = QgsCoordinateReferenceSystem("EPSG:4326")
        self.xform = QgsCoordinateTransform(crs_src, crs_dest, project.transformContext())

    def canvasMoveEvent(self, event):
        pt = self.toMapCoordinates(event.originalPixelPoint())
        ll = self.xform.transform(pt)
        self.statbar.showMessage(f'lat: {ll[1]:.6f}  lon: {ll[0]:.6f}')


class TrackViz(QMainWindow):
    assert os.path.isfile('/usr/bin/qgis'), 'couldnt find qgis in path'

    def __init__(self):
        # initialize qt app
        #self.app = QApplication(sys.argv)
        QgsApplication.setPrefixPath('/usr', True)
        self.qgs = QgsApplication([], True)

        super().__init__()
        self.qgs.initQgis()
        #self.qgs.setApplicationName('ais_track_viz')
        #self.qgs.setApplicationDisplayName('ais_track_viz')
        self.setWindowTitle('ais_track_viz')
        self.qgs.setMaxThreads(8)

        #self.bridge = QgsLayerTreeMapCanvasBridge(QgsProject.instance().layerTreeRoot(), self.canvas)
        self.project = QgsProject.instance()

        # create canvas window
        self.canvas = QgsMapCanvas()
        self.canvas.setCanvasColor(QtCore.Qt.black)
        self.canvas.enableAntiAliasing(True)
        self.setCentralWidget(self.canvas)
        self.toolbar = self.addToolBar("Canvas actions")

        # default projection
        crs_src = QgsCoordinateReferenceSystem("EPSG:4326")
        crs_dest = QgsCoordinateReferenceSystem("EPSG:3857")
        self.xform = QgsCoordinateTransform(crs_src, crs_dest, self.project.transformContext())

        # printlayout
        self.layout = QgsPrintLayout(self.project)
        self.layout.initializeDefaults()
        self.layout.setName('ais_track_viz')
        self.project.layoutManager().addLayout(self.layout)
        #self.basemap = QgsLayoutItemMap(self.layout)
        #self.basemap.attemptMove(QgsLayoutPoint(0, 0, QgsUnitTypes.LayoutMillimeters))
        #self.basemap.attemptResize(QgsLayoutSize(200, 200, QgsUnitTypes.LayoutMillimeters))
        #self.basemap.zoomToExtent(self.canvas.extent())
        #self.layout.addLayoutItem(self.basemap)

        # configs
        self.settings = self.canvas.mapSettings()
        self.settings.setFlag(QgsMapSettings.Antialiasing, True)
        #self.settings.setOutputSize(QSize(1920, 1080))
        #self.settings.setFlag(QgsMapSettings.DrawLabeling, False)

        # start the interface
        self.init_ui()


    def init_ui(self):
        # pan
        self.actionPan = QAction('pan', self)
        self.actionPan.setCheckable(True)
        self.actionPan.triggered.connect(self.pan)
        self.toolbar.addAction(self.actionPan)
        self.toolPan = QgsMapToolPan(self.canvas)
        self.toolPan.setAction(self.actionPan)

        # zoom in
        self.actionZoomIn = QAction('+', self)
        self.actionZoomIn.setCheckable(True)
        self.actionZoomIn.triggered.connect(self.zoomIn)
        self.toolbar.addAction(self.actionZoomIn)
        self.toolZoomIn = QgsMapToolZoom(self.canvas, False) # false = in
        self.toolZoomIn.setAction(self.actionZoomIn)
        
        # zoom out
        self.actionZoomOut = QAction('-', self)
        self.actionZoomOut.setCheckable(True)
        self.actionZoomOut.triggered.connect(self.zoomOut)
        self.toolbar.addAction(self.actionZoomOut)
        self.toolZoomOut = QgsMapToolZoom(self.canvas, True) # true = out
        self.toolZoomOut.setAction(self.actionZoomOut)

        # coord status
        self.statusBar().showMessage('show some status information here')
        self.actionCoord = QAction('locate coordinates', self)
        self.actionCoord.setCheckable(True)
        self.coordtool = toolCoord(self.canvas, self.statusBar(), self.project)
        self.actionCoord.triggered.connect(self.get_coord)
        self.toolbar.addAction(self.actionCoord)

        # load basemap layer 
        url = 'crs=EPSG:3857&format&tilePixelRatio=2&type=xyz&url=http://ecn.t3.tiles.virtualearth.net/tiles/a%7Bq%7D.jpeg?g%3D1&zmax=18&zmin=0'
        self.basemap_lyr = QgsRasterLayer(url, 'Bing Maps', 'wms')
        #self.basemap_lyr = QgsVectorLayer(url, 'Bing Maps', 'wms')
        assert self.basemap_lyr.isValid(), 'error loading basemap'
        self.project.addMapLayer(self.basemap_lyr)
        self.canvas.setExtent(self.basemap_lyr.extent())
        self.canvas.setLayers([self.basemap_lyr])

        # start the app
        self.show()
        #self.raise_()  # focus
        #self.app.exec_()  # blocking


    def zoomIn(self):
        if self.actionZoomIn.isChecked():
            self.canvas.setMapTool(self.toolZoomIn)
        else:
            self.canvas.unsetMapTool(self.toolZoomIn)


    def zoomOut(self):
        if self.actionZoomOut.isChecked():
            self.canvas.setMapTool(self.toolZoomOut)
        else:
            self.canvas.unsetMapTool(self.toolZoomOut)


    def pan(self):
        if self.actionPan.isChecked():
            self.canvas.setMapTool(self.toolPan)
        else:
            self.canvas.unsetMapTool(self.toolPan)


    def get_coord(self):
        if self.actionCoord.isChecked():
            self.canvas.setMapTool(self.coordtool)
        else:
            self.canvas.unsetMapTool(self.coordtool)
            self.statusBar().clearMessage()


    def split_feature_over_meridian(self, meridian, geom):
        adjust = lambda x: ((np.array(x) + 180) % 360) - 180  
        if isinstance(geom, str): geom = shapely.wkt.loads(geom)
        merged = shapely.ops.linemerge([geom.boundary, meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        splits = [Polygon(zip(adjust((p := next(decomp)).boundary.coords.xy[0]), p.boundary.coords.xy[1])),
                  Polygon(zip(np.abs(adjust((p := next(decomp)).boundary.coords.xy[0])), p.boundary.coords.xy[1])) ]
        return splits

    def layer_from_feature_old(self, geomwkt, ident, color=None, opacity=None, nosplit=False):
        if nosplit == False and (meridian := LineString(np.array(((-180, -180, 180, 180), (-90, 90, 90, -90))).T)).crosses(shapely.wkt.loads(geomwkt)):
            print(f'splitting {ident}')
            splits = self.split_feature_over_meridian(meridian, geomwkt)
            return [self.layer_from_feature(splits[0].wkt, ident+'A', color, opacity, nosplit=True)[0],
                    #self.layer_from_feature(splits[1].wkt.replace('-', ' '), ident+'B', color, opacity, nosplit=True)[0]]
                    self.layer_from_feature(splits[1].wkt, ident+'B', color, opacity, nosplit=True)[0]]
        if color is None: color = (colorhash(ident),)
        geomtype = geomwkt.split('(', 1)[0].rstrip().upper()
        if opacity is None: opacity = 0.5 if geomtype in ('POLYGON','LINEARRING') else 1
        vl = QgsVectorLayer(geomtype, str(ident), 'memory')
        pr = vl.dataProvider()
        seg = QgsFeature()
        seg.setGeometry(QgsGeometry.fromWkt(geomwkt))
        pr.addFeatures([seg])
        vl.updateExtents()
        symbol = QgsSymbol.defaultSymbol(vl.geometryType())
        symbol.setColor(QColor(*color if isinstance(color, tuple) else color))
        symbol.setOpacity(opacity)
        vl.renderer().setSymbol(symbol)
        return [vl]


    def linefeature(self, geom, ident, color=None, opacity=None, nosplit=False):
        meridian = LineString(np.array(((-180, -180, 180, 180), (-90, 90, 90, -90))).T)
        if nosplit == False and meridian.crosses(geom):
            print(f'splitting {ident}')
            splits = self.split_feature_over_meridian(meridian, geom)
            return [self.linefeature(splits[0], ident+'A', color, opacity, nosplit=True),
                    #self.layer_from_feature(splits[1].wkt.replace('-', ' '), ident+'B', color, opacity, nosplit=True)[0]]
                    self.linefeature(splits[1], ident+'B', color, opacity, nosplit=True)]

        if color is None: color = (colorhash(ident),)
        r = QgsRubberBand(self.canvas, True)

        if geom.type.upper() in ('POLYGON', 'LINEARRING'): 
            pts = [self.xform.transform(QgsPointXY(x,y)) for x,y in zip(*geom.boundary.coords.xy)]
            vector = QgsGeometry.fromPolygonXY([pts])
            r.setFillColor(QColor(*color))
            r.setStrokeColor(QColor(0,0,0))
            #r.setSecondaryStrokeColor(QColor(*color))
            r.setOpacity(opacity or 0.3)
            r.setWidth(2)
            #label = QgsLayoutItemLabel(self.layout)
            #label.setText(ident)
            #label.adjustSizeToText()
            #label.setPos(*r.getPoint(0))
            #label.setFontColor(QColor(255,0,0))
            #self.layout.addLayoutItem(label)

        elif geom.type.upper() == 'LINESTRING':
            pts = [self.xform.transform(QgsPointXY(x,y)) for x,y in zip(*geom.coords.xy)]
            vector = QgsGeometry.fromPolylineXY(pts)
            r.setColor(QColor(*color))
            r.setOpacity(opacity or 1)

        else: assert False, f'{geom.type} is not a linear feature!'

        r.setToGeometry(vector, None)
        #r.show()
        #r.updateCanvas()
        return r if nosplit else [r]
    '''
    self.canvas.scene().removeItem(r)
    '''

    def pointfeature():
        assert geom.type.upper() == 'MULTIPOINT'
        #r = QgsRubberBand(self.canvas, True)
        pts = [self.xform.transform(QgsPointXY(xy.x, xy.y)) for xy in geom]
        vector = QgsGeometry.fromMultiPointXY(pts)
        #lyr = self.project.addMapLayer()
        #r = QgsFeature()
        #r.setGeometry(vector)

        vl = QgsVectorLayer(geomtype, str(ident), 'memory')
        pr = vl.dataProvider()
        seg = QgsFeature()
        seg.setGeometry(vector)
        pr.addFeatures([seg])
        vl.updateExtents()
        symbol = QgsSymbol.defaultSymbol(vl.geometryType())
        symbol.setColor(QColor(*color))
        symbol.setOpacity(opacity or 1)
        vl.renderer().setSymbol(symbol)
        self.project.addMapLayer(vl)
        return vl


    def update_features(self, layers): 
        self.project.addMapLayers(layers)
        #QGuiApplication.processEvents()
        #self.qgs.processEvents()
        self.canvas.setLayers([self.basemap_lyr] + layers)


    def render_bg(self, layers, fname, w=1920, h=1080): 
        # https://gis.stackexchange.com/questions/245840/wait-for-canvas-to-finish-rendering-before-saving-image
        image = QImage(QSize(w,h), QImage.Format_RGB32)
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
        self.project.removeMapLayers([lyr.id() for lyr in self.settings.layers()])
        self.canvas.refresh()


    def exit(self):
        #self.project.write('scripts/ecoregion_test/testplot.qgz')
        self.project.removeMapLayers([lyr.id() for lyr in self.settings.layers()])
        self.canvas.close()
        self.close()
        self.qgs.closeAllWindows()
        #self.app.deleteLater()
        self.qgs.exitQgis()


    def export_qgis(self, projpath):
        #self.project.fileName()
        assert projpath[-4:] == '.qgz', 'project path must end with .qgz'
        self.project.write(projpath)


    def import_qgis(self, projpath='/home/matt/Desktop/qgis/ecoregion_test.qgz'):
        self.project.read(projpath)
        #self.layout = self.project.instance().layoutManager().layoutByName('scripted_layout')
        #self.layout.addItem(QgsLayoutItemMap(self.layout))


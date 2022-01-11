'''
vizualize AIS data and geometry features using QGIS. still a work in progress

https://docs.qgis.org/3.16/en/docs/pyqgis_developer_cookbook/intro.html

https://www.opengis.ch/2018/06/22/threads-in-pyqgis3/
https://gis.stackexchange.com/questions/245840/wait-for-canvas-to-finish-rendering-before-saving-image
# use processing framework plugin?
https://gis.stackexchange.com/questions/374567/in-python-what-is-the-best-way-to-replicate-the-qgis-heatmap-plugin
'''

import os
import sys
import shutil
from datetime import datetime, timedelta
from functools import partial, reduce
from hashlib import sha256
from multiprocessing import Pool

if (qgispath := shutil.which('qgis')):
    sys.path.append(qgispath)
else:
    print('warning: could not find QGIS installed!')

from qgis.PyQt.QtCore import Qt, QSize, QVariant, QTimer
from qgis.PyQt.QtGui import (
    QColor,
    QCursor,
    # QGuiApplication,
    # QImage,
    # QPainter,
    # QPainterPath,
)
from qgis.PyQt.QtWidgets import (
    QAction,
    # QApplication,
    # QFrame,
    # QLabel,
    QMainWindow,
    # QStatusBar,
    # QVBoxLayout,
    # QWidget,
)
from qgis.core import (
    #QgsMapRendererCustomPainterJob,
    QgsApplication,
    QgsCategorizedSymbolRenderer,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsFeature,
    QgsField,
    QgsFillSymbol,
    QgsGeometry,
    QgsLineSymbol,
    QgsMapRendererParallelJob,
    QgsMapSettings,
    QgsMarkerSymbol,
    QgsPointXY,
    QgsPrintLayout,
    # QgsProcessingFeedback,
    QgsProject,
    QgsRasterLayer,
    QgsRectangle,
    QgsRendererCategory,
    # QgsSymbol,
    QgsVectorFileWriter,
    QgsVectorLayer,
    QgsWkbTypes,
)
from qgis.gui import (
    QgsMapCanvas,
    # QgsMapCanvasItem,
    QgsMapToolEmitPoint,
    QgsMapToolPan,
    QgsMapToolZoom,
    QgsRubberBand,
    # QgsStatusBar,
    QgsVertexMarker,
)
#self.qgs = QgsApplication([], True)
pluginpath = '/usr/share/qgis/python/plugins'
assert os.path.isdir(pluginpath), 'could not find QGIS plugin path'
sys.path.append(pluginpath)
#import processing
#from processing.core.Processing import Processing
#Processing.initialize()

import numpy as np
import shapely.ops
import shapely.wkt
from shapely.geometry import LineString, Polygon, MultiPoint, Point

from common import output_dir
from gis import haversine

colorhash = lambda mmsi: f'#{sha256(str(mmsi).encode()).hexdigest()[-6:]}'
#ring2qwkt = lambda ring: f'POLYGON({ring.wkt.split(" ", 1)[1]})'
#hexatriad = lambda hexa: (hexa, f'{hexa[0]}{hexa[-2:]}{hexa[1:-2]}', f'{hexa[0]}{hexa[3:]}{hexa[1:3]}',)


class toolCoord(QgsMapToolEmitPoint):
    ''' custom map interface tool for retrieving coordinates at the cursor position '''

    def __init__(self, canvas, statbar, project):
        ''' initialize the coordinates map tool

            create statbar, define the coordinate reference systems and transformation context
        '''
        self.canvas = canvas
        super().__init__(self.canvas)
        self.statbar = statbar
        crs_src = QgsCoordinateReferenceSystem("EPSG:3857")
        crs_dest = QgsCoordinateReferenceSystem("EPSG:4326")
        self.xform = QgsCoordinateTransform(crs_src, crs_dest,
                                            project.transformContext())
        self.markers = []
        self.pts = []

    def canvasMoveEvent(self, event):
        ''' updates the status bar with the current coordinates when the canvas is moved '''
        xy = self.xform.transform(
            self.toMapCoordinates(event.originalPixelPoint()))
        self.statbar.showMessage(f'lat: {xy[1]:.6f}  lon: {xy[0]:.6f}')

    def canvasReleaseEvent(self, event):
        ''' place a marker at the current cursor location '''
        xy = self.toMapCoordinates(event.originalPixelPoint())
        m = QgsVertexMarker(self.canvas)
        m.setCenter(xy)
        m.setPenWidth(2)
        self.pts.append(self.xform.transform(xy))
        self.markers.append(m)


class toolScaleCoord(toolCoord):
    ''' add tools to the map to show the current displayed canvas scale in
        kilometers, and add event hooks to the canvas
    '''

    def canvasMoveEvent(self, event):
        ''' updates the status bar with the current coordinates when the canvas is moved '''
        ext = self.canvas.extent()
        xfr = self.xform.transform
        xy = xfr(ext.center())
        scale = haversine(*xfr(ext.xMinimum(), ext.yMinimum()),
                          *xfr(ext.xMaximum(), ext.yMaximum())) / 1000
        self.statbar.showMessage(
            f'center: lat {xy[1]:.6f}  lon {xy[0]:.6f}    scale: {int(scale - (scale % 1))}km'
        )

    def canvasReleaseEvent(self, event):
        ''' updates the status bar with the current coordinates when the canvas is released '''
        ext = self.canvas.extent()
        xfr = self.xform.transform
        xy = xfr(ext.center())
        scale = haversine(*xfr(ext.xMinimum(), ext.yMinimum()),
                          *xfr(ext.xMaximum(), ext.yMaximum())) / 1000
        self.statbar.showMessage(
            f'center: lat {xy[1]:.6f}  lon {xy[0]:.6f}    scale: {int(scale - (scale % 1))}km'
        )

    def getCurrentScale(self):
        ''' returns kilometers distance from the lower-left corner to upper-right corner of canvas '''
        ext = self.canvas.extent()
        xfr = self.xform.transform
        xy = xfr(ext.center())
        scale = haversine(*xfr(ext.xMinimum(), ext.yMinimum()),
                          *xfr(ext.xMaximum(), ext.yMaximum())) / 1000
        return int(scale - (scale % 1))

    def getCurrentCenter(self):
        ''' returns longitude / latitude coordinates from the center of the canvas '''
        ext = self.canvas.extent()
        xfr = self.xform.transform
        xy = xfr(ext.center())
        return tuple(xy)


class customQgsMultiPoint(QgsRubberBand):
    ''' custom MultiPoint geometry object using the PointGeometry WKB type '''

    def __init__(self, canvas):
        super().__init__(canvas, QgsWkbTypes.PointGeometry)


class ApplicationWindow(QMainWindow):
    ''' main application window

        runs the QGIS application using PyQt
    '''

    def __init__(self):
        ''' start the application

            initialize PyQt app, create a project and coordinate reference
            system, open a new map canvas window inside the main window, load
            map configurations, and then load the interface tools
        '''
        # start qgis
        QgsApplication.setPrefixPath('/usr', True)
        self.qgs = QgsApplication([], True)
        if not os.path.isdir(testpath := os.path.join(output_dir, 'png')):
            print(f'creating directory: {testpath}')
            os.mkdir(testpath)
        super().__init__()

        self.qgs.initQgis()
        # self.qgs.setApplicationName('ais_track_viz')
        # self.qgs.setApplicationDisplayName('ais_track_viz')
        self.setWindowTitle('ais_track_viz')
        self.qgs.setMaxThreads(8)

        # init project and coordinate reference system
        self.project = QgsProject.instance()
        crs_src = QgsCoordinateReferenceSystem("EPSG:4326")
        crs_dest = QgsCoordinateReferenceSystem("EPSG:3857")
        self.project.setCrs(crs_dest)
        self.xform = QgsCoordinateTransform(crs_src, crs_dest,
                                            self.project.transformContext())

        # create canvas window
        self.canvas = QgsMapCanvas()
        self.canvas.setCanvasColor(Qt.black)
        self.canvas.enableAntiAliasing(True)
        self.setCentralWidget(self.canvas)
        self.toolbar = self.addToolBar("Canvas actions")
        '''
        from qgis.gui import QgsLayerTreeMapCanvasBridge
        self.bridge = QgsLayerTreeMapCanvasBridge(QgsProject.instance().layerTreeRoot(), self.canvas)
        '''

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

        # keeping track of rendered items
        #self.features = []
        self.features_point = []
        self.features_line = []
        self.features_poly = []

        # start the interface
        #thread = threading.Thread(target=self.init_ui)
        #thread.start()
        #thread.join()
        self.init_ui()

        #Processing.initialize()

    def init_ui(self):
        ''' add some tools to the visualization window

            pan, zoom in/out, status bar, basemap layer (microsoft visual earth), etc.
        '''
        # pan
        self.actionPan = QAction('Pan', self)
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
        self.toolZoomIn = QgsMapToolZoom(self.canvas, False)  # false = in
        self.toolZoomIn.setAction(self.actionZoomIn)

        # zoom out
        self.actionZoomOut = QAction('-', self)
        self.actionZoomOut.setCheckable(True)
        self.actionZoomOut.triggered.connect(self.zoomOut)
        self.toolbar.addAction(self.actionZoomOut)
        self.toolZoomOut = QgsMapToolZoom(self.canvas, True)  # true = out
        self.toolZoomOut.setAction(self.actionZoomOut)

        # coord status
        self.statusBar().showMessage('')
        self.toolcoord = toolCoord(self.canvas, self.statusBar(), self.project)
        self.toolscalecoord = toolScaleCoord(self.canvas, self.statusBar(),
                                             self.project)
        self.actionCoord = QAction('Locate Coordinates', self)
        self.actionCoord.setCheckable(True)
        self.actionCoord.triggered.connect(self.get_coord)
        self.toolbar.addAction(self.actionCoord)
        self.actionClearCoord = QAction('Clear Markers', self)
        self.actionClearCoord.triggered.connect(self.clear_coord)
        self.toolbar.addAction(self.actionClearCoord)
        self.canvas.setMapTool(self.toolscalecoord)
        self.centralWidget().setCursor(QCursor())

        # load basemap layer
        url = 'crs=EPSG:3857&format&tilePixelRatio=2&type=xyz&url=http://ecn.t3.tiles.virtualearth.net/tiles/a%7Bq%7D.jpeg?g%3D1&zmax=18&zmin=0'
        self.basemap_lyr = QgsRasterLayer(url, 'Bing Maps', 'wms')
        assert self.basemap_lyr.isValid(), 'error loading basemap'
        self.project.addMapLayer(self.basemap_lyr)
        self.canvas.setExtent(self.basemap_lyr.extent())
        self.canvas.setLayers([self.basemap_lyr])

        # start the app
        self.show()

    def zoomIn(self):
        ''' toolbar zoom button action '''
        if self.actionZoomIn.isChecked():
            self.canvas.setMapTool(self.toolZoomIn)
        else:
            self.canvas.unsetMapTool(self.toolZoomIn)

    def zoomOut(self):
        ''' toolbar zoom button action '''
        if self.actionZoomOut.isChecked():
            self.canvas.setMapTool(self.toolZoomOut)
        else:
            self.canvas.unsetMapTool(self.toolZoomOut)

    def pan(self):
        ''' toolbar pan button action '''
        if self.actionPan.isChecked():
            self.canvas.setMapTool(self.toolPan)
        else:
            self.canvas.unsetMapTool(self.toolPan)

    def get_coord(self):
        ''' toolbar Place Marker button action '''
        if self.actionCoord.isChecked():
            self.canvas.unsetMapTool(self.toolscalecoord)
            self.canvas.setMapTool(self.toolcoord)
        else:
            self.canvas.unsetMapTool(self.toolcoord)
            self.canvas.setMapTool(self.toolscalecoord)
            self.centralWidget().setCursor(QCursor())

    def clear_coord(self):
        ''' toolbar Clear Markers button action '''
        for m in self.toolcoord.markers:
            self.canvas.scene().removeItem(m)
        self.toolcoord.markers = []
        self.toolcoord.pts = []

    def set_canvas_boundary(self, xmin=-180, ymin=-90, xmax=180, ymax=90):
        ''' set the map canvas boundary '''
        xmin = max(xmin, -180)
        xmax = min(xmax, 180)
        ymin = max(ymin, -90)
        ymax = min(ymax, 90)
        xy1 = self.xform.transform(QgsPointXY(xmin, ymin))
        xy2 = self.xform.transform(QgsPointXY(xmax, ymax))
        ext = QgsRectangle(xy1.x(), xy1.y(), xy2.x(), xy2.y())
        self.canvas.setExtent(ext)
        self.canvas.refresh()

    def focus_canvas_item(self, /, geom=None, domain=None, zone=None):
        ''' set the map canvas boundary to the boundary of the given object

            accepts one of:
                geom: shapely.geometry object
                zone: aisdb.gis.ZoneGeometry
                domain: aisdb.gis.Domain
        '''
        if domain is not None:
            self.set_canvas_boundary(xmin=domain.minX,
                                     ymin=domain.minY,
                                     xmax=domain.maxX,
                                     ymax=domain.maxY)
            return

        if zone is not None:
            geom = zone.Geometry

        if geom.type == 'LineString' or geom.type == 'Polygon':
            xmin = min(xmin, np.min(geometry.xy[0]))
            ymin = min(ymin, np.min(geometry.xy[1]))
            xmax = max(xmax, np.max(geometry.xy[0]))
            ymax = max(ymax, np.max(geometry.xy[1]))
        elif geom.type == 'MultiPoint':
            xmin = min(xmin, list(geometry)[0].x)
            ymin = min(ymin, list(geometry)[0].y)
            xmax = max(xmax, list(geometry)[0].x)
            ymax = max(ymax, list(geometry)[0].y)
        else:
            assert False, f'unknown geometry: {geom}'

        self.set_canvas_boundary(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)
        return

    def split_feature_over_meridian(self, meridian, geom):
        ''' '''
        adjust = lambda x: ((np.array(x) + 180) % 360) - 180
        if isinstance(geom, str): geom = shapely.wkt.loads(geom)
        merged = shapely.ops.linemerge([geom.boundary, meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        #splits = [Polygon(zip(adjust((p := next(decomp)).boundary.coords.xy[0]), p.boundary.coords.xy[1])),
        #          Polygon(zip(np.abs(adjust((p := next(decomp)).boundary.coords.xy[0])), p.boundary.coords.xy[1])) ]
        p1, p2 = next(decomp), next(decomp)
        splits = [
            Polygon(
                zip(adjust(p1.boundary.coords.xy[0]),
                    p1.boundary.coords.xy[1])),
            Polygon(
                zip(np.abs(adjust(p2.boundary.coords.xy[0])),
                    p2.boundary.coords.xy[1]))
        ]
        return splits

    def add_feature_point(self, geom, ident, color=None, opacity=None):
        ''' add a shapely.geometry.MultiPoint object to the map canvas '''
        if color is None: color = (colorhash(ident), )
        r = customQgsMultiPoint(self.canvas)
        if geom.type == 'MultiPoint' or geom.type == 'GeometryCollection':
            pts = [self.xform.transform(QgsPointXY(xy.x, xy.y)) for xy in geom]
            qgeom = QgsGeometry.fromMultiPointXY(pts)
        elif geom.type == 'Point':
            pts = self.xform.transform(QgsPointXY(geom.x, geom.y))
            qgeom = QgsGeometry.fromPointXY(pts)
        else:
            assert False
        r.setColor(QColor(*color))
        r.setOpacity(opacity or 1)
        r.setToGeometry(qgeom, None)
        self.features_point.append((ident, r))
        return

    def add_feature_line(self, geom, ident, color=None, opacity=None):
        ''' add a shapely.geometry.LineString object to the map canvas '''
        if color is None:
            color = (colorhash(ident), )
        r = QgsRubberBand(self.canvas, True)
        for i in range(0, len(geom.coords.xy[0]), 10000):
            pts = [
                self.xform.transform(QgsPointXY(x, y))
                for x, y in zip(geom.coords.xy[0][i:i + 10000],
                                geom.coords.xy[1][i:i + 10000])
            ]
            qgeom = QgsGeometry.fromPolylineXY(pts)
            r.setColor(QColor(*color))
            r.setOpacity(opacity or 1)
            r.setToGeometry(qgeom, None)
            self.features_line.append((ident, r))

    def add_feature_poly(self, geom, ident, color=None, opacity=None):
        ''' add a shapely.geometry.Polygon or
            shapely.geometry.LineString object to the map canvas '''

        if color is None:
            color = (colorhash(ident), )
        r = QgsRubberBand(self.canvas, True)
        if geom.type == 'LinearRing':
            pts = [
                self.xform.transform(
                    QgsPointXY(min(180, max(x, -180)), min(90, max(y, -90))))
                for x, y in zip(*geom.coords.xy)
            ]
        elif geom.type == 'Polygon':
            pts = [
                self.xform.transform(
                    QgsPointXY(min(180, max(x, -180)), min(90, max(y, -90))))
                for x, y in zip(*geom.boundary.coords.xy)
            ]
        elif geom.type == 'GeometryCollection':
            pts = reduce(list.__add__, [[
                self.xform.transform(
                    QgsPointXY(min(180, max(x, -180)), min(90, max(y, -90))))
                for x, y in zip(*g.boundary.coords.xy)
            ] for g in geom.geoms])
        else:
            assert False, f'unknown geometry type {geom.type}:{geom}'

        qgeom = QgsGeometry.fromPolygonXY([pts])
        r.setFillColor(QColor(*color))
        r.setStrokeColor(QColor(0, 0, 0))
        #r.setSecondaryStrokeColor(QColor(*color))
        r.setOpacity(opacity or 0.3)
        r.setWidth(2)
        #label = QgsLayoutItemLabel(self.layout)
        #label.setText(ident)
        #label.adjustSizeToText()
        #label.setPos(*r.getPoint(0))
        #label.setFontColor(QColor(255,0,0))
        #self.layout.addLayoutItem(label)
        r.setToGeometry(qgeom, None)
        self.features_poly.append((ident, r))

    def clear_points(self):
        ''' clear all Points objects from the map canvas '''
        while len(self.features_point) > 0:
            self.canvas.scene().removeItem(self.features_point.pop()[1])

    def clear_lines(self):
        ''' clear all LineString objects from the map canvas '''
        while len(self.features_line) > 0:
            self.canvas.scene().removeItem(self.features_line.pop()[1])

    def clear_polygons(self):
        ''' clear all Polygon and LinearRing objects from the map canvas '''
        while len(self.features_poly) > 0:
            self.canvas.scene().removeItem(self.features_poly.pop()[1])

    def clearfeatures(self):
        ''' clear all features from the map canvas '''
        self.clear_points()
        self.clear_lines()
        self.clear_polygons()

    def vectorize(self, identifiers, bands, color=None, opacity=None):
        ''' render vector layer for a given geometry, and return it as a QGIS vector layer


            identifier:
                unique descriptor of each in bands
            bands:
                QgsRubberBand object describing geometry to be vectorized
            color:
                can be an RGBA tuple, hexadecimal code, or color name string
                if no color is chosen, a new color will be determined using a
                hash of the identifier
            opacity:
                float ranging from 0 to 1. If set to None, will use a preset value


        '''

        geomtype = QgsWkbTypes.displayString(bands[0].asGeometry().wkbType())
        if geomtype == 'LineString':
            symboltype = QgsLineSymbol
            if not opacity: opacity = 1
        elif geomtype == 'MultiPoint':
            symboltype = QgsMarkerSymbol
            if not opacity: opacity = 1
        elif geomtype in ('LinearRing', 'Polygon', 'MultiPolygon'):
            symboltype = QgsFillSymbol
            if not opacity: opacity = 0.33
            if not color: color = '#bbbbbb'
        else:
            assert False, f'unknown type {geomtype}'

        # assign hex color hash for each identifier
        categories = []
        for ident in identifiers:
            sym = symboltype.createSimple({'identifier': ident})
            #sym.setColor(QColor(*color if isinstance(color, tuple) else color or colorhash(ident)))
            sym.setColor(QColor(color or colorhash(ident)))
            sym.setOpacity(opacity)
            cat = QgsRendererCategory(str(ident),
                                      symbol=sym.clone(),
                                      label='identifier',
                                      render=True)
            categories.append(cat)
        styles = QgsCategorizedSymbolRenderer(attrName='identifier',
                                              categories=categories)
        '''
            from qgis.core import QgsHeatmapRenderer,QgsStyle

            # show color ramp names
            QgsStyle().defaultStyle().colorRampNames()
            print(styles.dump()[0:1000])


            styles = QgsHeatmapRenderer()
            styles.setRadius(5)
            #styles.setColorRamp(QgsStyle().defaultStyle().colorRamp('Spectral'))
            styles.setColorRamp(QgsStyle().defaultStyle().colorRamp('Blues'))
            styles.setWeightExpression(None)
        '''

        #geomtype = QgsWkbTypes.displayString(int(bands[0].asGeometry().wkbType()))
        vl = QgsVectorLayer(geomtype + '?crs=epsg:3857', f'tmp_lyr', 'memory')
        pr = vl.dataProvider()
        pr.addAttributes([
            QgsField('identifier', QVariant.Int),
        ])
        vl.updateFields()
        features = []
        for qgeom, ident in zip(bands, identifiers):
            ft = QgsFeature()
            ft.setGeometry(qgeom.asGeometry())
            ft.setFields(pr.fields())
            ft.setAttribute(0, ident)
            features.append(ft)
        pr.addFeatures(features)
        vl.updateExtents()
        vl.setRenderer(styles)
        vl.triggerRepaint()
        vl.setAutoRefreshEnabled(True)

        return vl

    def render_vectors(self, fname='test.png', w=1920, h=1080):
        ''' get the currently displayed geometry objects, render them as vectors, and save the image as .png

            the canvas will be cleared after saving the image
        '''
        '''
        from qgis.core import *
        identifiers, bands = np.array(self.features_poly).T
        import numpy as np
        w=1920
        h=1080
        fname='test.png'
        self= viz
        '''
        #self.canvas.setLayers([self.basemap_lyr])
        #self.project.instance().removeMapLayers([lyr.id() for lyr in layers])

        self.vl1 = None
        self.vl2 = None
        self.vl3 = None
        if len(self.features_point) > 0:
            #self.vl1 = self.vectorize([ft[0] for ft in self.features_point], [ft[1].asGeometry() for ft in self.features_point])
            self.vl1 = self.vectorize(
                *np.array(self.features_point, dtype=object).T)
            #self.vl1.setName('points')
        if len(self.features_line) > 0:
            self.vl2 = self.vectorize(
                *np.array(self.features_line, dtype=object).T)
            #self.vl2 = self.vectorize([ft[0] for ft in self.features_line], [ft[1].asGeometry() for ft in self.features_line])
            #self.vl2.setName('lines')
        if len(self.features_poly) > 0:
            self.vl3 = self.vectorize(
                *np.array(self.features_poly, dtype=object).T)
            #self.vl3 = self.vectorize([ft[0] for ft in self.features_poly], [ft[1].asGeometry() for ft in self.features_poly])
            #self.vl3.setName('polygons')
        '''
        self.canvas.saveAsImage(f'output{os.path.sep}{fname}')
        '''

        #self.project.write(f'output{os.path.sep}state.qgz')
        #self.canvas.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        self.canvas.update()
        self.canvas.refresh()
        self.settings.setLayers([
            vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr]
            if vl is not None
        ])
        self.settings.setExtent(
            self.canvas.extent())  # seems redundant but is actually necessary
        self.settings.setOutputSize(QSize(w, h))

        render = QgsMapRendererParallelJob(self.settings)

        def finished():
            img = render.renderedImage()
            imgpath = os.path.join(output_dir, 'png', fname)
            img.save(imgpath, 'png')

        render.finished.connect(finished)
        render.start()
        render.waitForFinished()
        #print(os.path.join(output_dir, 'png', fname))

        self.canvas.setLayers([self.basemap_lyr])
        '''
            self.canvas.setLayers([
                #*(f[1] for f in self.features_point),
                #*(f[1] for f in self.features_line),
                #*(f[1] for f in self.features_poly),
                #*(lyr for lyr in (self.vl1, self.vl2, self.vl3) if lyr is not None),
                self.basemap_lyr,
            ])
            #job = QgsMapRendererCustomPainterJob(self.settings, painter)
            #job.renderSynchronously()
            render = QgsMapRendererParallelJob(settings)


            painter.end()
            image.save(f'output{os.path.sep}{fname}')
        '''

    def export_vlayer_as_shp(self, vl, fpath=f'output{os.path.sep}test.shp'):
        ''' currently unused. prefer exporting shapes in WKB format using shapely '''
        save_opt = QgsVectorFileWriter.SaveVectorOptions()
        save_opt.driverName = 'ESRI Shapefile'
        save_opt.fileEncoding = 'UTF-8'
        transform_context = self.project.instance().transformContext()
        result = QgsVectorFileWriter.writeAsVectorFormatV2(
            vl, fpath,
            self.project.instance().transformContext(), save_opt)
        assert not result[0], f'{result}'
        return True

    def export_qgis(self, projpath):
        ''' export QGIS project instance to the given project path'''
        #self.project.fileName()
        assert projpath[-4:] == '.qgz', 'project path must end with .qgz'
        self.project.write(projpath)

    def import_qgis(self,
                    projpath='/home/matt/Desktop/qgis/ecoregion_test.qgz'):
        ''' import QGIS project instance from the given project path'''
        self.project.read(projpath)
        #self.layout = self.project.instance().layoutManager().layoutByName('scripted_layout')
        #self.layout.addItem(QgsLayoutItemMap(self.layout))

    def exit(self):
        ''' remove canvas items and shut down QGIS '''
        self.clearfeatures()
        #self.project.write('scripts/ecoregion_test/testplot.qgz')
        self.project.removeMapLayers(
            [lyr.id() for lyr in self.settings.layers()])
        self.canvas.close()
        self.qgs.closeAllWindows()
        #self.app.deleteLater()
        self.qgs.exitQgis()
        self.close()


def serialize_geomwkb(tracks):
    ''' for each track dictionary, serialize the geometry as WKB to the output directory '''
    wkbdir = os.path.join(output_dir, 'wkb/')
    if not os.path.isdir(wkbdir):
        os.mkdir(wkbdir)

    for track in tracks:
        if len(track['time']) == 1:
            geom = MultiPoint([
                Point(x, y, t)
                for x, y, t in zip(track['lon'], track['lat'], track['time'])
            ])
        else:
            geom = LineString(zip(track['lon'], track['lat'], track['time']))
        fname = os.path.join(
            wkbdir,
            f'mmsi={track["mmsi"]}_epoch={int(track["time"][0])}-{int(track["time"][-1])}_{geom.type}.wkb'
        )
        with open(fname, 'wb') as f:
            f.write(geom.wkb)

    return


#def blocking_io(fpath):
#    for x in trackgen(deserialize_generator(fpath)):
#        yield x


def cpu_bound(track, domain, cutdistance, maxdistance, cuttime, minscore):
    timesplit = partial(segment_tracks_timesplits, maxdelta=cuttime)
    distsplit = partial(segment_tracks_encode_greatcircledistance,
                        cutdistance=cutdistance,
                        maxdistance=maxdistance,
                        cuttime=cuttime,
                        minscore=minscore)
    geofenced = partial(fence_tracks, domain=domain)
    split_len = partial(max_tracklength, max_track_length=10000)
    print('processing mmsi', track['mmsi'], end='\r')
    serialize_geomwkb(split_len(distsplit(timesplit([track]))))
    return


def serialize_geoms(tracks,
                    domain,
                    processes,
                    cutdistance=5000,
                    maxdistance=125000,
                    cuttime=timedelta(hours=6),
                    minscore=0.0001):
    with Pool(processes=processes) as p:
        fcn = partial(cpu_bound,
                      domain=domain,
                      cutdistance=cutdistance,
                      maxdistance=maxdistance,
                      cuttime=cuttime,
                      minscore=minscore)
        p.imap_unordered(fcn, tracks, chunksize=1)
        p.close()
        p.join()
    print()

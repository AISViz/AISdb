import os
import sys
from hashlib import sha256

#from qgis.core import QgsApplication, QgsProject, QgsRasterLayer, QgsPrintLayout, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsMapSettings, QgsPointXY, QgsGeometry, QgsWkbTypes, QgsTask, QgsVectorLayer, QgsField, QgsFeature, QgsMapRendererCustomPainterJob, QgsSymbol, QgsCategorizedSymbolRenderer, QgsRendererCategory, QgsLineSymbol
from qgis.core import *
from qgis.PyQt.QtCore import Qt, QSize, QVariant, QTimer
from qgis.gui import QgsMapCanvas, QgsMapToolPan, QgsMapToolZoom, QgsRubberBand, QgsMapCanvasItem, QgsVertexMarker, QgsMapToolEmitPoint, QgsStatusBar, QgsMapCanvasItem
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
        reference: https://github.com/NationalSecurityAgency/qgis-latlontools-plugin/blob/master/copyLatLonTool.py
    '''

    def __init__(self, canvas, statbar, project):
        self.canvas = canvas
        super().__init__(self.canvas)
        self.statbar = statbar
        crs_src = QgsCoordinateReferenceSystem("EPSG:3857")
        crs_dest = QgsCoordinateReferenceSystem("EPSG:4326")
        self.xform = QgsCoordinateTransform(crs_src, crs_dest, project.transformContext())
        self.markers = []
        self.pts = []

    def canvasMoveEvent(self, event):
        xy = self.xform.transform(self.toMapCoordinates(event.originalPixelPoint()))
        self.statbar.showMessage(f'lat: {xy[1]:.6f}  lon: {xy[0]:.6f}')

    def canvasReleaseEvent(self, event):
        xy = self.toMapCoordinates(event.originalPixelPoint())
        m = QgsVertexMarker(self.canvas)
        m.setCenter(xy)
        m.setPenWidth(2)
        self.pts.append(self.xform.transform(xy))
        self.markers.append(m)


class customQgsMultiPoint(QgsRubberBand):
    def __init__(self, canvas):
        super().__init__(canvas, QgsWkbTypes.PointGeometry)

'''
https://www.opengis.ch/2018/06/22/threads-in-pyqgis3/
class renderLineFeature(QgsTask):
    def __init__(self, canvas, xform, geom, ident, color=None, opacity=None, nosplit=False):
        super().__init__(str(ident), QgsTask.CanCancel)
        if color is None: color = (colorhash(ident),)
        #self.context = context
        self.canvas = canvas
        self.xform = xform
        self.geom = geom
        self.ident = ident
        self.color = color
        self.opacity = opacity
        self.nosplit = nosplit
        #self.feat = None
        self.exception = None

    def run(self):
        #r = QgsRubberBand(self.canvas, True)
        try:
            pts = [self.xform.transform(QgsPointXY(x,y)) for x,y in zip(*self.geom.boundary.coords.xy)]
            self.qgeom = QgsGeometry.fromPolygonXY([pts])
        except Exception as e:
            self.exception = e
            return False

        return True

    def finished(self, result):
        if result:
            feat = QgsRubberBand(self.canvas, True)
            #self.canvas.update()
            #r.show()
            #r.updateCanvas()
            #self.features.append(self.feat)
            #return r if nosplit else [r]
            feat.setFillColor(QColor(*self.color))
            feat.setStrokeColor(QColor(0,0,0))
            #r.setSecondaryStrokeColor(QColor(*color))
            feat.setOpacity(self.opacity or 0.3)
            feat.setWidth(2)
            feat.setToGeometry(self.qgeom, None)
        else:
            raise self.exception
        return 

'''


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

        # init project and coordinate reference system
        self.project = QgsProject.instance()
        crs_src = QgsCoordinateReferenceSystem("EPSG:4326")
        crs_dest = QgsCoordinateReferenceSystem("EPSG:3857")
        self.project.setCrs(crs_dest)
        self.xform = QgsCoordinateTransform(crs_src, crs_dest, self.project.transformContext())

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


    def init_ui(self):
        ''' add some tools to the visualization window '''
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
        self.statusBar().showMessage('')
        self.toolcoord = toolCoord(self.canvas, self.statusBar(), self.project)
        self.actionCoord = QAction('locate coordinates', self)
        self.actionCoord.setCheckable(True)
        self.actionCoord.triggered.connect(self.get_coord)
        self.toolbar.addAction(self.actionCoord)
        self.actionClearCoord = QAction('clear markers', self)
        self.actionClearCoord.triggered.connect(self.clear_coord)
        self.toolbar.addAction(self.actionClearCoord)

        # load basemap layer 
        url = 'crs=EPSG:3857&format&tilePixelRatio=2&type=xyz&url=http://ecn.t3.tiles.virtualearth.net/tiles/a%7Bq%7D.jpeg?g%3D1&zmax=18&zmin=0'
        self.basemap_lyr = QgsRasterLayer(url, 'Bing Maps', 'wms')
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
            self.canvas.setMapTool(self.toolcoord)
        else:
            self.canvas.unsetMapTool(self.toolcoord)
            self.statusBar().clearMessage()

    
    def clear_coord(self):
        for m in self.toolcoord.markers: self.canvas.scene().removeItem(m)
        self.toolcoord.markers = []
        self.toolcoord.pts = []


    def poly_from_coords(self):
        geom = Polygon(
                  [(p.x(), p.y()) for p in self.toolcoord.pts] 
                + [(self.toolcoord.pts[0].x(), self.toolcoord.pts[0].y())]
            )
        self.clear_coord()
        return geom


    def split_feature_over_meridian(self, meridian, geom):
        adjust = lambda x: ((np.array(x) + 180) % 360) - 180  
        if isinstance(geom, str): geom = shapely.wkt.loads(geom)
        merged = shapely.ops.linemerge([geom.boundary, meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        splits = [Polygon(zip(adjust((p := next(decomp)).boundary.coords.xy[0]), p.boundary.coords.xy[1])),
                  Polygon(zip(np.abs(adjust((p := next(decomp)).boundary.coords.xy[0])), p.boundary.coords.xy[1])) ]
        return splits


    def add_feature_point(self, geom, ident, color=None, opacity=None):
        if color is None: color = (colorhash(ident),)
        assert geom.type.upper() == 'MULTIPOINT'
        r = customQgsMultiPoint(self.canvas)
        pts = [self.xform.transform(QgsPointXY(xy.x, xy.y)) for xy in geom]
        qgeom = QgsGeometry.fromMultiPointXY(pts)
        r.setColor(QColor(*color))
        r.setOpacity(opacity or 1)
        r.setToGeometry(qgeom, None)
        #return r
        self.features_point.append((ident, r))
        return


    def add_feature_polyline(self, geom, ident, color=None, opacity=None, nosplit=False):
        #task = renderLineFeature(self.canvas, self.xform, geom, ident, color, opacity, nosplit)
        #self.qgs.taskManager().addTask(task)
        meridian = LineString(np.array(((-180, -180, 180, 180), (-90, 90, 90, -90))).T)
        if nosplit == False and meridian.crosses(geom):
            print(f'splitting {ident}')
            splits = self.split_feature_over_meridian(meridian, geom)
            self.add_feature_polyline(splits[0], ident+'A', color, opacity, nosplit=True)
            #self.layer_from_feature(splits[1].wkt.replace('-', ' '), ident+'B', color, opacity, nosplit=True)[0]
            self.add_feature_polyline(splits[1], ident+'B', color, opacity, nosplit=True)
            return

        if color is None: color = (colorhash(ident),)
        r = QgsRubberBand(self.canvas, True)

        if geom.type.upper() in ('POLYGON', 'LINEARRING'): 
            if geom.type == 'LinearRing':
                pts = [self.xform.transform(QgsPointXY(x,y)) for x,y in zip(*geom.coords.xy)]
            elif geom.type == 'Polygon':
                pts = [self.xform.transform(QgsPointXY(x,y)) for x,y in zip(*geom.boundary.coords.xy)]
            qgeom = QgsGeometry.fromPolygonXY([pts])
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
            qgeom = QgsGeometry.fromPolylineXY(pts)
            r.setColor(QColor(*color))
            r.setOpacity(opacity or 1)

        else: assert False, f'{geom.type} is not a linear feature!'

        r.setToGeometry(qgeom, None)
        #self.canvas.update()
        #r.show()
        #r.updateCanvas()
        if geom.type.upper() in ('POLYGON', 'LINEARRING'): 
            self.features_poly.append((ident, r))
        elif geom.type.upper() == 'LINESTRING':
            self.features_line.append((ident, r))

        return 


    def clear_points(self):
        while len(self.features_point) > 0: self.canvas.scene().removeItem(self.features_point.pop()[1])


    def clear_lines(self):
        while len(self.features_line) > 0: self.canvas.scene().removeItem(self.features_line.pop()[1])


    def clear_polygons(self):
        while len(self.features_poly) > 0: self.canvas.scene().removeItem(self.features_poly.pop()[1])


    def clearfeatures(self):
        self.clear_points()
        self.clear_lines()
        self.clear_polygons()


    def vectorize(self, qgeoms, identifiers, color=None, opacity=None, nosplit=False):
        '''
        self=viz
        ident = identifiers[0]
        qgeoms = [ft[1].asGeometry() for ft in self.features_line]
        qgeoms = [ft[1].asGeometry() for ft in self.features_point]
        identifiers = [1 for x in qgeoms]
        '''

        geomtype = QgsWkbTypes.displayString(qgeoms[0].wkbType())
        if geomtype == 'LineString':
            symboltype = QgsLineSymbol
            if not opacity: opacity = 1
        elif geomtype == 'MultiPoint':
            symboltype = QgsMarkerSymbol
            if not opacity: opacity = 1
        elif geomtype in ('LinearRing', 'Polygon', 'MultiPolygon'):
            symboltype = QgsFillSymbol
            if not opacity: opacity = 0.33
        else:
            assert False, f'unknown type {geomtype}'

        unique, idx = np.unique(identifiers, return_index=True)

        categories = []
        #styles = QgsCategorizedSymbolRenderer()
        for ident, qgeom in zip(unique, np.array(qgeoms)[idx]):
            #sym = QgsSymbol.defaultSymbol(int(qgeom.wkbType()))
            sym = symboltype.createSimple({'identifier': ident})
            sym.setColor(QColor(color or colorhash(ident)))
            sym.setOpacity(opacity)
            cat = QgsRendererCategory(str(ident), symbol=sym.clone(), label='identifier', render=True)
            #styles.addCategory(cat)
            categories.append(cat)
        styles = QgsCategorizedSymbolRenderer(attrName='identifier', categories=categories)


        '''
        QgsStyle().defaultStyle().colorRampNames()

        
        styles = QgsHeatmapRenderer()
        styles.setColorRamp(QgsStyle().defaultStyle().colorRamp('Spectral'))

        print(styles.dump()[0:1000])
        '''

        #if color is None: color = colorhash(ident)
        #if opacity is None: opacity = 0.5 if geomtype in ('Polygon','LineString') else 1


        #meridian = LineString(np.array(((-180, -180, 180, 180), (-90, 90, 90, -90))).T)
        geomtype = QgsWkbTypes.displayString(int(qgeoms[0].wkbType()))
        vl = QgsVectorLayer(geomtype+'?crs=epsg:3857', f'tmp_lyr', 'memory')
        #vl.setCrs(self.project.crs())
        pr = vl.dataProvider()
        pr.addAttributes([
                QgsField('identifier', QVariant.Int), 
            ])
        vl.updateFields()
        features = []
        for qgeom,ident in zip(qgeoms[1:], identifiers[1:]):
            ft = QgsFeature()
            ft.setGeometry(qgeom)
            ft.setFields(pr.fields())
            ft.setAttribute(0, ident)
            features.append(ft)
            #pr.addFeatures([ft])
        pr.addFeatures(features)
        vl.updateExtents()
        vl.setRenderer(styles)
        vl.triggerRepaint()
        vl.setAutoRefreshEnabled(True)

        '''
        vl.extent()
        tft = vl.getFeature(1)

        symbol = QgsSymbol.defaultSymbol(vl.geometryType())
        symbol.setColor(QColor(*color if isinstance(color, tuple) else color))
        symbol.setOpacity(opacity)
        vl.renderer().setSymbol(symbol)
        '''
        return vl


    def render_vectors(self, fname='test.png', w=1920, h=1080): 
        # https://gis.stackexchange.com/questions/245840/wait-for-canvas-to-finish-rendering-before-saving-image
        '''
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
            self.vl1 = self.vectorize([ft[1].asGeometry() for ft in self.features_point], [ft[0] for ft in self.features_point])
        if len(self.features_line) > 0:
            self.vl2 = self.vectorize([ft[1].asGeometry() for ft in self.features_line],  [ft[0] for ft in self.features_line])
        if len(self.features_poly) > 0:
            self.vl3 = self.vectorize([ft[1].asGeometry() for ft in self.features_poly],  [ft[0] for ft in self.features_poly])
    
        #self.clearfeatures()
        #self.settings.setLayers(self.settings.layers() + [vl])
        #self.project.addMapLayers([vl2])
        '''

        self.canvas.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        #self.canvas.setExtent(vl.extent())
        #self.project.write(f'output{os.path.sep}state.qgz')
        self.canvas.update()
        self.canvas.refresh()
        #QGuiApplication.processEvents()
        #self.qgs.processEvents()
        self.canvas.saveAsImage(f'output{os.path.sep}{fname}')
        '''
        #settings = QgsMapSettings()
        self.canvas.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        self.settings.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        #self.settings.setLayers([self.vl1, self.basemap_lyr])
        self.canvas.update()
        self.canvas.refresh()
        self.settings.setOutputSize(QSize(w, h))
        self.settings.setExtent(self.canvas.extent())
        #QgsMapLayerRegistry.instance()
        #self.settings.setOutputSize(QSize(w,h))

        render = QgsMapRendererParallelJob(self.settings)
        def finished():
            img = render.renderedImage()
            img.save(f'output{os.path.sep}{fname}', 'png')
        render.finished.connect(finished)
        render.start()
        render.waitForFinished()

        '''
        #image = QImage(QSize(w,h), QImage.Format_RGB32)
        #painter = QPainter(image)
        #self.settings.setLayers([vl, self.basemap_lyr])
        #self.canvas.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        #self.settings.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        settings = QgsMapSettings()
        settings.setLayers([vl for vl in [self.vl1, self.vl2, self.vl3, self.basemap_lyr] if vl is not None])
        #job = QgsMapRendererCustomPainterJob(self.settings, painter)
        #job.renderSynchronously()
        render = QgsMapRendererParallelJob(settings)

        render.start()
        render.waitForFinished()
        img = render.renderedImage()
        img.save(f'output{os.path.sep}{fname}', 'png')
        

        painter.end()
        image.save(f'output{os.path.sep}{fname}')
        '''


    def export_vlayer_as_shp(self, vl, fpath=f'output{os.path.sep}test.shp'):
        save_opt = QgsVectorFileWriter.SaveVectorOptions()
        save_opt.driverName = 'ESRI Shapefile'
        save_opt.fileEncoding = 'UTF-8'
        transform_context = self.project.instance().transformContext()
        result = QgsVectorFileWriter.writeAsVectorFormatV2(
                vl, 
                fpath,
                self.project.instance().transformContext(),
                save_opt
            )
        assert not result[0], f'{result}'
        return True


    def export_qgis(self, projpath):
        #self.project.fileName()
        assert projpath[-4:] == '.qgz', 'project path must end with .qgz'
        self.project.write(projpath)


    def import_qgis(self, projpath='/home/matt/Desktop/qgis/ecoregion_test.qgz'):
        self.project.read(projpath)
        #self.layout = self.project.instance().layoutManager().layoutByName('scripted_layout')
        #self.layout.addItem(QgsLayoutItemMap(self.layout))


    def exit(self):
        self.clearfeatures()
        #self.project.write('scripts/ecoregion_test/testplot.qgz')
        self.project.removeMapLayers([lyr.id() for lyr in self.settings.layers()])
        self.canvas.close()
        self.qgs.closeAllWindows()
        #self.app.deleteLater()
        self.qgs.exitQgis()
        self.close()

'''
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
        """
        seg.setGeometry(q)
        """ 
        pr.addFeatures([seg])
        vl.updateExtents()
        symbol = QgsSymbol.defaultSymbol(vl.geometryType())
        symbol.setColor(QColor(*color if isinstance(color, tuple) else color))
        symbol.setOpacity(opacity)
        vl.renderer().setSymbol(symbol)
        return [vl]
'''


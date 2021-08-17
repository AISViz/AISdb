from gis import *

from qgis.core import QgsPolygon, QgsLineString, QgsPointXY

def test_point_in_polygon():
    zones = zones_from_txts_old('../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', 'east')
    x, y  = zones['geoms']['Z1'].boundary.coords.xy
    xi, yi = x[1], y[1]

    geom = ZoneGeom(name='Z1', x=x, y=y)
    geometry = QgsPolygon(QgsLineString(list(QgsPointXY(xi, yi) for xi, yi in zip(x,y))))
    geometry = Polygon(zip(x, y))

    pt = QgsPointXY(xi, yi)
    geom.geometry.boundingBox().contains(pt)

    geom.serialize()[1]


def test_domain():
    '''
    zonegeoms = {k:ZoneGeom(k, *v.boundary.coords.xy) for k,v in zones['geoms'].items()}
    '''
    domain = Domain('east', zonegeoms)
    dist_to_centroids = domain.nearest_polygons_to_point(-64, 45)
    dist_to_centroids
    domain.point_in_polygon(-64, 45)

    for key in domain.geoms.keys():
        print(key, 
                domain.geoms[key].geometry.boundingBox().contains(QgsPointXY(-64,45)),
                domain.point_in_pol
                )

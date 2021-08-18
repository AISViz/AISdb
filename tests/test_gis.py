from gis import *

#from qgis.core import QgsPolygon, QgsLineString, QgsPointXY

def test_point_in_polygon():
    zones_dir = '../scripts/dfo_project/EastCoast_EEZ_Zones_12_8'
    shapefilepaths = sorted([os.path.abspath(os.path.join( zones_dir, f)) for f in os.listdir(zones_dir) if 'txt' in f])
    zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
    #domain = Domain('east', zonegeoms)

    #zones = zones_from_txts_old('../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', 'east')

    x, y  = domain.geoms['Z12'].x, domain.geoms['Z12'].y
    xi, yi = x[1], y[1]

    geom = ZoneGeom(name='Z12', x=x, y=y)
    geometry = QgsPolygon(QgsLineString(list(QgsPointXY(xi, yi) for xi, yi in zip(x,y))))
    geometry = Polygon(zip(x, y))

    pt = QgsPointXY(xi, yi)
    geom.geometry.boundingBox().contains(pt)

    geom.serialize()[1]


def test_domain():
    '''
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

from aisdb.gis import *


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

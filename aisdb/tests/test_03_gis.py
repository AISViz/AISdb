from shapely.geometry import Polygon

from aisdb.gis import Domain, DomainFromPoints
from aisdb.tests.create_testing_data import random_polygons_domain
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox


def test_domain():
    domain = random_polygons_domain(count=10)
    dist_to_centroids = domain.nearest_polygons_to_point(-64, 45)

    print(dist_to_centroids)

    for name, zone in domain.zones.items():
        poly = zone['geometry']
        # assert poly.is_valid
        print(poly.type, end='|')
    print()

    zoneID = domain.point_in_polygon(zone['geometry'].centroid.x,
                                     zone['geometry'].centroid.y)
    print(f'{zoneID = }')

    print(f'{domain.minX=}\n{domain.maxX=}\n{domain.minY=}\n{domain.maxY=}')


def test_DomainFromPoints():

    domain = DomainFromPoints([(-45, 50), (-50, 35), (-40, 55)],
                              [10000, 1000, 100000])


def test_domain_points_in_polygon():
    lon, lat = sample_gulfstlawrence_bbox()
    z1 = Polygon(zip(lon, lat))
    z2 = Polygon(zip(lon + 90, lat))
    z3 = Polygon(zip(lon, lat - 45))
    domain = Domain('gulf domain',
                    zones=[
                        {
                            'name': 'z1',
                            'geometry': z1
                        },
                        {
                            'name': 'z2',
                            'geometry': z2
                        },
                        {
                            'name': 'z3',
                            'geometry': z3
                        },
                    ])

    xx = [z1.centroid.x, z2.centroid.x, z3.centroid.x]
    yy = [z1.centroid.y, z2.centroid.y, z3.centroid.y]
    test = [domain.point_in_polygon(x, y) for x, y in zip(xx, yy)]
    assert test[0] == 'z1'
    assert test[1] == 'z2'
    assert test[2] == 'z3'

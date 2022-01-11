from aisdb.gis import Domain
from tests.create_testing_data import zonegeoms_or_randompoly


def test_domain_fakedata():
    zonegeoms = zonegeoms_or_randompoly(randomize=True, count=10)

    domain = Domain('0test', zonegeoms, cache=False)
    dist_to_centroids = domain.nearest_polygons_to_point(-64, 45)
    print(dist_to_centroids)

    for key in domain.geoms.keys():
        poly = domain.geoms[key].geometry
        # assert poly.is_valid
        print(poly.type, end='|')
    print()

    zoneID = domain.point_in_polygon(*domain.geoms[key].centroid)
    print(f'{zoneID = }')

    print(f'{domain.minX=}\n{domain.maxX=}\n{domain.minY=}\n{domain.maxY=}')


def test_domain_realdata():
    zonegeoms = zonegeoms_or_randompoly(randomize=False)

    domain = Domain('0test', zonegeoms, cache=False)
    dist_to_centroids = domain.nearest_polygons_to_point(-64, 45)
    print(dist_to_centroids)

    for key in domain.geoms.keys():
        poly = domain.geoms[key].geometry
        # assert poly.is_valid
        print(poly.type, end='|')
    print()

    # will be Z0 if outside of all zones (or empty geoms list)
    zoneID = domain.point_in_polygon(*domain.geoms[key].centroid)
    print(f'{zoneID = }\t{len(domain.geoms.keys()) = }')

    print(f'{domain.minX=}\n{domain.maxX=}\n{domain.minY=}\n{domain.maxY=}')

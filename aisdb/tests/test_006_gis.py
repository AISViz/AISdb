import os
import zipfile

import numpy as np
from shapely.geometry import Polygon

from aisdb.gis import (Domain, DomainFromPoints, DomainFromTxts, distance3D, shiftcoord, )
from aisdb.tests.create_testing_data import random_polygons_domain
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox


def test_invalid_domain():
    try:
        Domain("errordomain",
               [{"name": "outofbounds0", "geometry": Polygon(zip([-200, -170, -170, -200, -200], [90, 90, 0, 0, 90]))},
                   {"name": "outofbounds1",
                       "geometry": Polygon(zip([200, 170, 170, 200, 200], [-90, -90, 0, 0, -90]))}])
    except ValueError:
        return
    except Exception as e:
        raise (e)

    raise RuntimeError("This test should fail with a ValueError")


def test_domain():
    domain = random_polygons_domain(count=10)
    dist_to_centroids = domain.nearest_polygons_to_point(-64, 45)

    print(dist_to_centroids)

    for name, zone in domain.zones.items():
        poly = zone["geometry"]
        print(poly.geom_type, end="|")
    print()

    zoneID = domain.point_in_polygon(zone["geometry"].centroid.x, zone["geometry"].centroid.y)
    print(f"{zoneID = }")

    print(f"{domain.minX=}\n{domain.maxX=}\n{domain.minY=}\n{domain.maxY=}")


def test_DomainFromTxts():
    folder = os.path.join(os.path.dirname(__file__), "test_zones")
    zipf = os.path.join(folder, "test_zones.zip")

    with zipfile.ZipFile(zipf, "r") as zip_ref:
        members = list(set(zip_ref.namelist()) - set(sorted(os.listdir(folder))))
        zip_ref.extractall(path=folder, members=members)

    domain = DomainFromTxts(domainName="test", folder=folder)
    assert domain


def test_DomainFromPoints():
    domain = DomainFromPoints([(-45, 50), (-50, 35), (-40, 55)], [10000, 1000, 100000])
    assert domain


def test_domain_points_in_polygon():
    lon, lat = sample_gulfstlawrence_bbox()
    z1 = Polygon(zip(lon, lat))
    z2 = Polygon(zip(lon - 145, lat))
    z3 = Polygon(zip(lon, lat - 45))
    domain1 = Domain("gulf domain", zones=[{"name": "z1", "geometry": z1}])

    xx = [z1.centroid.x, z2.centroid.x, z3.centroid.x]
    yy = [z1.centroid.y, z2.centroid.y, z3.centroid.y]
    test = [domain1.point_in_polygon(x, y) for x, y in zip(xx, yy)]
    assert test[0] == "z1"
    assert test[1] == "Z0"
    assert test[2] == "Z0"


def test_shiftcoord():
    x = np.array([-360, -270, -180, -90, 0, 90, 180, 270, 360])
    xshift = shiftcoord(x)
    assert sum(xshift == np.array([0, 90, 180, -90, 0, 90, -180, -90, 0])) == 9

    x2 = np.array([-200, -190, -181, -180, -179, -170, -160])
    xshift2 = shiftcoord(x2)


def test_distance3D():
    x1, y1 = -45, 50
    x2, y2 = -40, 50
    depth_metres = -500
    dist = distance3D(x1, y1, x2, y2, depth_metres)

import os
import zipfile

import geopandas as gpd
import numpy as np
from shapely.geometry import MultiPolygon, Polygon

from aisdb.gis import (
    Domain,
    DomainFromPoints,
    DomainFromTxts,
    distance3D,
    shiftcoord,
)
from aisdb.tests.create_testing_data import random_polygons_domain
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox


def test_invalid_domain():
    try:
        Domain(
            "errordomain",
            [
                {
                    "name": "outofbounds0",
                    "geometry": Polygon(
                        zip([-200, -170, -170, -200, -200], [90, 90, 0, 0, 90])
                    ),
                },
                {
                    "name": "outofbounds1",
                    "geometry": Polygon(
                        zip([200, 170, 170, 200, 200], [-90, -90, 0, 0, -90])
                    ),
                },
            ],
        )
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

    zoneID = domain.point_in_polygon(
        zone["geometry"].centroid.x, zone["geometry"].centroid.y
    )
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


def _sample_multipolygon():
    p1 = Polygon([(-64, 45), (-64, 46), (-63, 46), (-63, 45), (-64, 45)])
    p2 = Polygon([(-62, 44), (-62, 45), (-61, 45), (-61, 44), (-62, 44)])
    return MultiPolygon([p1, p2])


def test_domain_multipolygon_zone():
    mp = _sample_multipolygon()
    domain = Domain("multidomain", zones=[{"name": "mz", "geometry": mp}])
    assert set(domain.zones.keys()) == {"mz_0", "mz_1"}
    assert domain.minX == -64
    assert domain.maxX == -61
    assert domain.minY == 44
    assert domain.maxY == 46
    assert domain.point_in_polygon(-63.5, 45.5) == "mz_0"
    assert domain.point_in_polygon(-61.5, 44.5) == "mz_1"


def test_domain_from_geodataframe():
    mp = _sample_multipolygon()
    single = Polygon([(-60, 43), (-60, 44), (-59, 44), (-59, 43), (-60, 43)])
    gdf = gpd.GeoDataFrame(
        {"zone_name": ["multi", "single"]}, geometry=[mp, single], crs="EPSG:4326"
    )
    domain = Domain.from_geodataframe(gdf, name_column="zone_name", name="gdfdomain")
    assert domain.name == "gdfdomain"
    assert set(domain.zones.keys()) == {"multi_0", "multi_1", "single"}


def test_domain_from_geodataframe_reprojects():
    single = Polygon([(-60, 43), (-60, 44), (-59, 44), (-59, 43), (-60, 43)])
    gdf = gpd.GeoDataFrame(
        {"zone_name": ["single"]}, geometry=[single], crs="EPSG:4326"
    )
    gdf_mercator = gdf.to_crs(epsg=3857)
    domain = Domain.from_geodataframe(gdf_mercator, name_column="zone_name")
    assert set(domain.zones.keys()) == {"single"}
    assert abs(domain.minX - -60) < 1e-6
    assert abs(domain.maxX - -59) < 1e-6
    assert abs(domain.minY - 43) < 1e-6
    assert abs(domain.maxY - 44) < 1e-6


def test_domain_from_geodataframe_default_names():
    mp = _sample_multipolygon()
    gdf = gpd.GeoDataFrame(geometry=[mp], crs="EPSG:4326")
    domain = Domain.from_geodataframe(gdf)
    assert set(domain.zones.keys()) == {"zone_0_0", "zone_0_1"}

''' geometry and GIS related utilities '''

import os
from datetime import datetime, timedelta
from functools import partial

import numpy as np
import shapely.wkb
from shapely.ops import unary_union
from shapely.geometry import Polygon, Point, LineString
from shapely.geometry.collection import GeometryCollection

from common import dbpath
from index import index

shiftcoord = lambda x, rng=360: ((np.array(x) + (rng / 2)) % 360) - (rng / 2)


def dt_2_epoch(dt_arr, t0=datetime(1970, 1, 1, 0, 0, 0)):
    ''' convert datetime.datetime to epoch minutes '''
    delta = lambda dt: (dt - t0).total_seconds()
    if isinstance(dt_arr, (list, np.ndarray)):
        return np.array(list(map(float, map(delta, dt_arr))))
    elif isinstance(dt_arr, (datetime)):
        return int(delta(dt_arr))
    else:
        raise ValueError('input must be datetime or array of datetimes')


def epoch_2_dt(ep_arr, t0=datetime(1970, 1, 1, 0, 0, 0), unit='seconds'):
    ''' convert epoch minutes to datetime.datetime '''
    delta = lambda ep, unit: t0 + timedelta(**{f'{unit}': ep})
    if isinstance(ep_arr, (list, np.ndarray)):
        return np.array(list(map(partial(delta, unit=unit), ep_arr)))
    elif isinstance(ep_arr, (float, int)):
        return delta(ep_arr, unit=unit)
    else:
        raise ValueError('input must be integer or array of integers')


def haversine(x1, y1, x2, y2):
    ''' https://en.wikipedia.org/wiki/Haversine_formula '''
    x1r, y1r, x2r, y2r = map(np.radians, [x1, y1, x2, y2])
    dlon, dlat = x2r - x1r, y2r - y1r
    return 6367 * 2 * np.arcsin(
        np.sqrt(
            np.sin(dlat / 2.)**2 +
            np.cos(y1r) * np.cos(y2r) * np.sin(dlon / 2.)**2)) * 1000


def delta_meters(track, rng):
    return np.array(
        list(
            map(haversine, track['lon'][rng][:-1], track['lat'][rng][:-1],
                track['lon'][rng][1:], track['lat'][rng][1:])))


def delta_seconds(track, rng):
    return np.array(
        list(
            #(track['time'][rng][1:] - track['time'][rng][:-1]))) * 60
            (track['time'][rng][1:] - track['time'][rng][:-1])))


def delta_knots(track, rng=None):
    if rng is None:
        rng = range(len(track['time']))
    return delta_meters(track, rng) / delta_seconds(track, rng) * 1.9438445


def delta_reported_knots(track, rng):
    ''' difference between reported SOG and great circle distance computed SOG '''
    knots = delta_knots(track, rng)
    return np.abs((
        (track['sog'][rng][mask][:-1] + track['sog'][rng][end][1:]) / 2) -
                  knots)


def dms2dd(d, m, s, ax):
    ''' convert degrees, minutes, seconds to decimal degrees '''
    dd = float(d) + float(m) / 60 + float(s) / (60 * 60)
    if (ax == 'W' or ax == 'S') and dd > 0: dd *= -1
    return dd


def strdms2dd(strdms):
    '''  convert string representation of degrees, minutes, seconds to decimal deg '''
    d, m, s, ax = [v for v in strdms.replace("''", '"').split(' ') if v != '']
    return dms2dd(float(d.rstrip('°')), float(m.rstrip("'")),
                  float(s.rstrip('"')), ax.upper())


def radial_coordinate_boundary(x, y, radius=100000):
    ''' checks maximum coordinate range for a given point and radial distance in meters '''

    xmin, xmax = x, x
    ymin, ymax = y, y

    # TODO: compute precise value instead of approximating
    while haversine(x, y, xmin, y) < radius:
        xmin -= 0.05
    while haversine(x, y, xmax, y) < radius:
        xmax += 0.05
    while haversine(x, y, x, ymin) < radius:
        ymin -= 0.05
    while haversine(x, y, x, ymax) < radius:
        ymax += 0.05

    return {
        'xmin': xmin,
        'xmax': xmax,
        'ymin': ymin,
        'ymax': ymax,
    }


class ZoneGeom():
    ''' class describing polygon coordinate geometry

        geometry will be stored as a shapely.geometry.Polygon object.
        some additional variables are stored as attributes, such as
        centroids and farthest radial distance from the centroid

        When compared with a shapely.geometry.Point object, the '>' operator
        will return True if the point is contained within the polygon

        args:
            name: string
                unique descriptor of a zone
            x: np.array
                longitude coordinate array
            y: np.array
                latitude coordinate array
    '''

    def __init__(self, name, x, y):
        self.name = name
        self.x = np.array(x)
        self.y = np.array(y)
        self.geometry = Polygon(zip(x, y))
        self.centroid = (self.geometry.centroid.x, self.geometry.centroid.y)
        self.maxradius = next(
            np.max(
                haversine(*self.centroid, *xy) for xy in zip(self.x, self.y)))
        self.minX, self.maxX = np.min(self.x), np.max(self.x)
        self.minY, self.maxY = np.min(self.y), np.max(self.y)
        if not (self.minX >= -180 and self.maxX <= 180):
            print(
                f'warning: zone {self.name} boundary exceeds longitudes -180..180'
            )
            meridian = LineString(
                np.array(((-180, -180, 180, 180), (-90, 90, 90, -90))).T)
            self.centroid = shiftcoord(self.centroid[0]), self.centroid[1]
            merged = shapely.ops.linemerge([self.geometry.boundary, meridian])
            border = shapely.ops.unary_union(merged)
            decomp = list(shapely.ops.polygonize(border))
            p1, p2 = decomp[0], decomp[-1]
            splits = [
                Polygon(
                    zip(shiftcoord(p2.boundary.coords.xy[0]),
                        p2.boundary.coords.xy[1])),
                Polygon(
                    zip(shiftcoord(p1.boundary.coords.xy[0]),
                        p1.boundary.coords.xy[1]))
            ]
            #Polygon(zip(shiftcoord(np.array(p2.boundary.coords.xy[0])), p2.boundary.coords.xy[1])) ]
            #self.geometry = unary_union(splits)
            self.geometry = GeometryCollection(splits)

        if not (self.minY <= 90 and self.maxY >= -90):
            print(
                f'warning: zone {self.name} boundary exceeds latitudes -90..90'
            )

    def __gt__(self, xy):
        return (self.minX <= xy[0] <= self.maxX
                and self.minY <= xy[1] <= self.maxY
                and self.geometry.contains(Point(*xy)))


class ZoneGeomFromTxt(ZoneGeom):
    ''' constructor class to initialize zone geometry from a text file '''

    def __init__(self, txt):
        name = txt.rsplit(os.path.sep, 1)[1].split('.')[0]
        with open(txt, 'r') as f:
            pts = f.read()
        xy = list(
            map(float,
                pts.replace('\n\n', '').replace('\n', ',').split(',')[:-1]))
        super().__init__(name, xy[::2], xy[1::2])


class Domain():
    ''' collection of ZoneGeom objects, with additional computed statistics such as zone set boundary coordinates

        args:
            name: string
                name to describe collection of ZoneGeom objects
            geoms: list
                collection of ZoneGeom objects
            cache: boolean
                if True, Domains will be cached as binary in the database.
                A hash of Domain.name will be used as the primary key
            clearcache: boolean
                if True, the contents of the cache will be cleared before storing
                new values in the cache

        attr:
            self.name
            self.geoms
            self.minX
            self.minY
            self.maxX
            self.maxY

    '''

    def __init__(self, name=None, geoms={}, cache=True, clearcache=False):
        if len(geoms.keys()) == 0:
            assert False, 'domain needs to have atleast one polygon geometry'
        self.name = name
        self.geoms = geoms
        if cache:
            # check cache for domain hash
            dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
            with index(bins=False,
                       store=True,
                       storagedir=dbdir,
                       filename=dbname) as domaincache:
                if clearcache:
                    seed = domaincache.hash_seed(
                        callback=self.init_boundary,
                        passkwargs={"name": self.name})
                    domaincache.drop_hash(seed=seed)
                self.bounds = domaincache(callback=self.init_boundary,
                                          name=self.name)[0]
        else:
            self.bounds = self.init_boundary(name=name)

        self.minX, self.minY, self.maxX, self.maxY = self.bounds.convex_hull.bounds
        self.minX -= 1
        self.maxX += 1
        self.minY -= 1
        self.maxY += 1

    def init_boundary(self, name):
        if sum([
                g.geometry.type == 'GeometryCollection'
                for g in self.geoms.values()
        ]) > 0:
            print('warning: domain exceeded map boundary')
        return unary_union([
            g.geometry for g in self.geoms.values()
            if g.geometry.type != 'GeometryCollection'
        ])

    def nearest_polygons_to_point(self, x, y):
        ''' compute great circle distance for this point to each polygon centroid,
            subtracting the maximum polygon radius.
            returns all zones with distances less than zero meters, sorted by
            nearest first
        '''
        #dist_to_centroids = {k : haversine(x, y, *g.centroid) - g.maxradius for k,g in self.geoms.items()}
        dist_to_centroids = {}
        for k, g in self.geoms.items():
            dist_to_centroids.update(
                {k: haversine(x, y, *g.centroid) - g.maxradius})
        return {
            k: v
            for k, v in sorted(dist_to_centroids.items(),
                               key=lambda item: item[1]) if v <= 0
        }

    def point_in_polygon(self, x, y):
        ''' returns the first polygon that contains the given point.
            uses coarse filtering by precomputing distance to centroids
        '''
        nearest = self.nearest_polygons_to_point(x, y)
        for key, val in nearest.items():
            if self.geoms[key] > (x, y):
                return key
        return 'Z0'


'''

class SerializedZoneGeom(ZoneGeom):

    def deserialize(self):
        pass
        return name, x, y

    def __init__(self, serial):
        super().__init__(*self.deserialize(serial))

    def __repr__(self):
        return self.name, self.geometry.asWkt()

    def binary(self):
        return self.name, bytes(self.geometry.asWkb())
'''
"""
def zones_from_txts_old(dirpath='../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', domain='east'):
    from shapely.geometry import Polygon
    from shapely.ops import unary_union
    dirpath, dirnames, filenames = np.array(list(os.walk(dirpath)), dtype=object).T
    txts = list(map(lambda txt: f'{dirpath[0]}/{txt}', sorted(filter(lambda f: f[-3:] == 'txt', filenames[-1]))))
    merge = lambda *arr: np.concatenate(np.array(*arr).T)
    zones = {'domain': domain, 'geoms': {}}
    for txt in txts:
        with open(txt, 'r') as f: pts = f.read()
        xy = list(map(float, pts.replace('\n\n', '').replace('\n',',').split(',')[:-1]))
        zones['geoms'][txt.rsplit(os.path.sep,1)[1].split('.')[0]] = Polygon(zip(xy[::2], xy[1::2]))
    zones['hull'] = unary_union(zones['geoms'].values()).convex_hull
    zones['hull_xy'] = merge(zones['hull'].boundary.coords.xy)
    return zones


def parse_zones_from_txts(dbpath, dirpath='../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', domain='east'):

    from database.create_tables import sqlite_create_table_polygons
    import pickle
    aisdb = dbconn(dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    sqlite_create_table_polygons(aisdb.cur)

    _, dirnames, filenames = np.array(list(os.walk(dirpath)), dtype=object).T
    txts = list(map(lambda txt: f'{dirpath}/{txt}', sorted(filter(lambda f: f[-3:] == 'txt', filenames[-1]))))
    #merge = lambda *arr: np.concatenate(np.array(*arr).T)
    #zones = {'domain': domain, 'geoms': {}}
    for txt in txts:
        with open(txt, 'r') as f: pts = f.read()
        xy = list(map(float, pts.replace('\n\n', '').replace('\n',',').split(',')[:-1]))
        minX, maxX, minY, maxY = min(xy[::2]), max(xy[::2]), min(xy[1::2]), max(xy[1::2])
        #zones['geoms'][txt.rsplit(os.path.sep,1)[1].split('.')[0]] =
        name = txt.rsplit(os.path.sep,1)[1].split('.')[0]
        poly = Polygon(zip(xy[::2], xy[1::2]))
        row = (minX, maxX, minY, maxY, name, domain, pickle.dumps(poly))
        cur.execute('''
            INSERT INTO rtree_polygons (minX, maxX, minY, maxY, objname, domain, binary)
            VALUES (?,?,?,?,?,?,?) ''', row)
    #zones['hull'] = unary_union(zones['geoms'].values()).convex_hull
    #zones['hull_xy'] = merge(zones['hull'].boundary.coords.xy)
    #return zones
    conn.commit()
"""

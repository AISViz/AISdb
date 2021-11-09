import os
from datetime import timedelta
from collections import UserDict
from functools import reduce
import json

import numpy as np
import shapely.wkb
from shapely.ops import unary_union
from shapely.geometry import Polygon, Point

from common import dbpath
from index import index


def haversine(x1, y1, x2, y2):
    ''' https://en.wikipedia.org/wiki/Haversine_formula '''
    x1r,y1r,x2r,y2r = map(np.radians, [x1,y1,x2,y2])
    dlon,dlat = x2r - x1r, y2r - y1r
    return 6367 * 2 * np.arcsin(np.sqrt(np.sin(dlat/2.)**2 + np.cos(y1r) * np.cos(y2r) * np.sin(dlon/2.)**2)) * 1000


def delta_meters(track, rng):
    return np.array(list(map(haversine, track['lon'][rng][:-1], track['lat'][rng][:-1], track['lon'][rng][1:], track['lat'][rng][1:])))


def delta_seconds(track, rng):
    return np.array(list((track['time'][rng][1:] - track['time'][rng][:-1]))) * 60


def delta_knots(track, rng):
    return delta_meters(track, rng) / delta_seconds(track, rng) * 1.9438445


def delta_reported_knots(track, rng):
    ''' difference between reported SOG and great circle distance computed SOG '''
    knots = delta_knots(track, rng)
    return np.abs(((track['sog'][rng][mask][:-1] + track['sog'][rng][end][1:]) / 2) - knots)


def dms2dd(d, m, s, ax):
    ''' convert degrees, minutes, seconds to decimal degrees '''
    dd = float(d) + float(m)/60 + float(s)/(60*60);
    if (ax == 'W' or ax == 'S') and dd > 0: dd *= -1
    return dd


def strdms2dd(strdms):
    '''  convert string representation of degrees, minutes, seconds to decimal deg '''
    d, m, s, ax = [v for v in strdms.split(' ') if v != '']
    return dms2dd(
            float(d.rstrip('°')),
            float(m.rstrip("'")),
            float(s.rstrip('"')),
            ax.upper()
        )


class ZoneGeom():

    def __init__(self, name, x, y):
        self.name = name
        self.x = np.array(x)
        self.y = np.array(y)
        self.geometry = Polygon(zip(x, y))
        self.centroid = self.geometry.centroid
        self.maxradius = next(np.max(haversine(self.centroid.x, self.centroid.y, xi, yi) for xi,yi in zip(self.x, self.y)))
        self.minX, self.maxX = np.min(self.x), np.max(self.x)
        self.minY, self.maxY = np.min(self.y), np.max(self.y)
        if not (self.minX >= -180 and self.maxX <= 180):
            print(f'warning: zone {self.name} boundary exceeds longitudes -180..180')
        if not (self.minY <= 90 and self.maxY >= -90): 
            print(f'warning: zone {self.name} boundary exceeds latitudes -90..90')

    def __gt__(self, xy):
        return self.geometry.contains(Point(*xy))


class ZoneGeomFromTxt(ZoneGeom):
    ''' constructor class to initialize zone geometry from a text file '''

    def __init__(self, txt):
        name = txt.rsplit(os.path.sep,1)[1].split('.')[0]
        with open(txt, 'r') as f: pts = f.read()
        xy = list(map(float, pts.replace('\n\n', '').replace('\n',',').split(',')[:-1]))
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

    '''

    def __init__(self, name=None, geoms=[], cache=True, clearcache=False):
        self.name = name
        self.geoms = geoms
        if cache: 
            # check cache for domain hash
            dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
            with index(bins=False, store=True, storagedir=dbdir, filename=dbname) as domaincache:
                if clearcache:
                    seed = domaincache.hash_seed(callback=self.init_boundary, passkwargs={"name":self.name})
                    domaincache.drop_hash(seed=seed)
                self.bounds = domaincache(callback=self.init_boundary, name=self.name)[0]

            self.minX, self.minY, self.maxX, self.maxY = self.bounds.convex_hull.bounds

        else:
            self.bounds = self.init_boundary(name=name)
            self.minX, self.minY, self.maxX, self.maxY = self.bounds.convex_hull.bounds

    def init_boundary(self, name):
        return unary_union([g.geometry for g in self.geoms.values()])

    def nearest_polygons_to_point(self, x, y):
        ''' compute great circle distance for this point to each polygon centroid, 
            subtracting the maximum polygon radius. 
            returns all zones with distances less than zero meters, sorted by 
            nearest first
        '''
        dist_to_centroids = {k : haversine(x, y, g.centroid.x, g.centroid.y) - g.maxradius for k,g in self.geoms.items()}
        return {k:v for k,v in sorted(dist_to_centroids.items(), key=lambda item: item[1]) if v <= 0}

    def point_in_polygon(self, x, y):
        ''' returns the first polygon that contains the given point. 
            uses coarse filtering by precomputing distance to centroids
        '''
        nearest = self.nearest_polygons_to_point(x, y)
        for key in nearest.keys():
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

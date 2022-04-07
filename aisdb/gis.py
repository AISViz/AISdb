''' geometry and GIS related utilities '''

import os
from datetime import datetime, timedelta
from functools import partial

import numpy as np
import shapely.wkb
import shapely.ops
from shapely.geometry import Polygon, LineString, Point

from aisdb.proc_util import glob_files

# shiftcoord = lambda x, rng=360: ((np.array(x) + (rng / 2)) % 360) - (rng / 2)


def shiftcoord(x, rng=180):
    ''' Correct longitude coordinates to be within range(-180, 180).
        For latitude coordinate correction, set rng to 90.
    '''
    assert len(x) > 0, 'x must be array-like'
    if not isinstance(x, np.ndarray):
        x = np.array(x)
    shift_idx = np.where(np.abs(x) != 180)[0]
    for idx in shift_idx:
        x[idx] = ((x[idx] + rng) % 360) - rng
    flip_idx = np.where(np.abs(x) == 180)[0]
    for idx in flip_idx:
        x[idx] *= -1
    assert (rng * -1 <= np.all(x) <= rng)
    return x


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

    delta = lambda ep, unit: t0 + timedelta(**{unit: ep})

    if isinstance(ep_arr, (list, np.ndarray)):
        return np.array(list(map(partial(delta, unit=unit), map(int, ep_arr))))

    elif isinstance(ep_arr, (float, int)):
        return delta(ep_arr, unit=unit)

    else:
        raise ValueError('input must be integer or array of integers')


def haversine(x1, y1, x2, y2):
    ''' https://en.wikipedia.org/wiki/Haversine_formula '''
    x1r, y1r, x2r, y2r = map(np.radians, [x1, y1, x2, y2])
    dlon, dlat = x2r - x1r, y2r - y1r
    return 6371.088 * 2 * np.arcsin(
        np.sqrt(
            np.sin(dlat / 2.)**2 +
            np.cos(y1r) * np.cos(y2r) * np.sin(dlon / 2.)**2)) * 1000


def delta_meters(track, rng=None):
    rng = range(len(track['time'])) if rng is None else rng
    return np.array(
        list(
            map(haversine, track['lon'][rng][:-1], track['lat'][rng][:-1],
                track['lon'][rng][1:], track['lat'][rng][1:])))


def delta_seconds(track, rng=None):
    rng = range(len(track['time'])) if rng is None else rng
    return np.array(list((track['time'][rng][1:] - track['time'][rng][:-1])))


def delta_knots(track, rng=None):
    rng = range(len(track['time'])) if rng is None else rng
    ds = np.array([np.max((1, s)) for s in delta_seconds(track, rng)],
                  dtype=object)
    return delta_meters(track, rng) / ds * 1.9438445


def radial_coordinate_boundary(x, y, radius=100000):
    ''' checks maximum coordinate range for a given point and radial distance in meters '''

    xmin, xmax = x, x
    ymin, ymax = y, y

    # TODO: compute precise value instead of approximating
    while haversine(x, y, xmin, y) < radius:
        xmin -= 0.001
    while haversine(x, y, xmax, y) < radius:
        xmax += 0.001
    while haversine(x, y, x, ymin) < radius:
        ymin -= 0.001
    while haversine(x, y, x, ymax) < radius:
        ymax += 0.001

    return {
        'xmin': xmin,
        'xmax': xmax,
        'ymin': ymin,
        'ymax': ymax,
    }


def distance3D(x1, y1, x2, y2, depth_metres):
    ''' haversine/pythagoras approximation of vessel distance to
        point at given depth
    '''
    a2 = haversine(x1=x1, y1=y1, x2=x2, y2=y2)**2
    b2 = depth_metres**2
    c2 = a2 + b2
    return np.sqrt(c2)


def vesseltrack_3D_dist(tracks, x1, y1, z1):
    ''' appends approximate distance to point at every position

        x1 (float)
            point longitude
        y1 (float)
            point latitude
        z1 (float)
            point depth (metres)

    '''
    for track in tracks:
        track['dynamic'] = track['dynamic'].union(set(['distance_metres']))
        dists = [
            distance3D(x1=x1, y1=y1, x2=x, y2=y, depth_metres=z1)
            for x, y in zip(track['lon'], track['lat'])
        ]
        track['distance_metres'] = np.array(dists, dtype=object)
        yield track


class Domain():
    ''' collection of zone geometry dictionaries, with additional
        statistics such as hull bounding box

    args:
        name: string
            Domain name
        zones: list of dictionaries
            Collection of zone geometry dictionaries.
            Must have keys 'name' (string) and 'geometry'
            (shapely.geometry.Polygon)

    attr:
        self.name
        self.zones
        self.bounds
        self.minX
        self.minY
        self.maxX
        self.maxY

    '''

    def __init__(self, name, zones=[]):
        if len(zones) == 0:
            assert False, 'domain needs to have atleast one polygon geometry'
        self.name = name
        self.zones = zones
        assert hasattr(self, 'minX')
        assert hasattr(self, 'minY')
        assert hasattr(self, 'maxX')
        assert hasattr(self, 'maxY')
        assert self.minX is not None
        assert self.maxX is not None
        assert self.minY is not None
        assert self.maxY is not None

    def nearest_polygons_to_point(self, x, y):
        ''' compute great circle distance for this point to each polygon centroid,
            subtracting the maximum polygon radius.
            returns all zones with distances less than zero meters, sorted by
            nearest first
        '''
        dist_to_centroids = {}
        for z in self.zones:
            dist_to_centroids.update({
                z['name']:
                haversine(x, y, *z['geometry'].centroid.xy) - z['maxradius']
            })
        return dist_to_centroids

    def point_in_polygon(self, x, y):
        ''' returns the first polygon that contains the given point.
            uses coarse filtering by precomputing distance to centroids
        '''
        nearest = self.nearest_polygons_to_point(x, y)
        for zone in self.zones:
            if zone['name'] not in nearest.keys():
                continue
            if zone['geometry'].contains(Point(x, y)):
                return zone['name']
        return 'Z0'


class DomainFromTxts(Domain):
    meridian = LineString(
        np.array((
            (-180, -180, 180, 180),
            (-90, 90, 90, -90),
        )).T)

    def adjust_coords(self, longitudes):
        '''
            longitudes = np.array([-181, -179, 0, 179, 181])
        '''
        longitudes[np.where(longitudes >= 180)[0]] = -180
        longitudes[np.where(longitudes <= -180)[0]] = 180
        return longitudes

    def split_geom(self, geom):
        merged = shapely.ops.linemerge([geom.boundary, self.meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        return decomp

    def __init__(self, domainName, folder):
        self.minX = None
        self.maxX = None
        self.minY = None
        self.maxY = None
        files = glob_files(folder, ext='txt')
        zones = []
        for txt in files:
            filename = txt.rsplit(os.path.sep, 1)[1].split('.')[0]
            with open(txt, 'r') as f:
                pts = f.read()
            xy = list(
                map(float,
                    pts.replace('\n\n', '').replace('\n',
                                                    ',').split(',')[:-1]))
            x, y = np.array(xy[::2]), np.array(xy[1::2])
            if self.minX is None or np.min(x) < self.minX:
                self.minX = np.min(x)
            if self.maxX is None or np.max(x) > self.maxX:
                self.maxX = np.max(x)
            if self.minY is None or np.min(y) < self.minY:
                self.minY = np.min(y)
            if self.maxY is None or np.max(y) > self.maxY:
                self.maxY = np.max(y)
            geom = Polygon(zip(x, y))
            minX, maxX = np.min(x), np.max(x)
            if not (minX >= -180 and maxX <= 180):
                for g in self.split_geom(geom):
                    if g.centroid.x < -180:
                        x, y = np.array(g.boundary.coords.xy)
                        zones.append({
                            'name': filename,
                            'geometry': Polygon(zip(shiftcoord(x), y))
                        })
                    elif g.centroid.x > 180:
                        x, y = np.array(g.boundary.coords.xy)
                        x[np.where(x <= 180)] = -180.
                        zones.append({
                            'name': filename,
                            'geometry': Polygon(zip(shiftcoord(x), y))
                        })
                    else:
                        zones.append({'name': filename, 'geometry': g})
            else:
                zones.append({'name': filename, 'geometry': geom})

        for zone in zones:
            zone['maxradius'] = np.max([
                haversine(*zone['geometry'].centroid.xy, x2=x, y2=y)
                for x, y in zip(*zone['geometry'].boundary.coords.xy)
            ])

        super().__init__(domainName, zones)

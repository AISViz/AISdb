'''Geometry and GIS utilities'''

import os
from datetime import datetime, timedelta
from functools import partial

import numpy as np
#import shapely.wkb
import shapely.ops
import shapely.geometry
import warnings
from shapely.geometry import Polygon, LineString, Point

from aisdb.aisdb import haversine
from aisdb.proc_util import glob_files


def shiftcoord(x, rng=180):
    ''' Correct longitude coordinates to be within range(-180, 180).
        For latitude coordinate correction, set rng to 90.
    '''
    assert len(x) > 0, 'x must be array-like'
    if not isinstance(x, np.ndarray):
        x = np.array(x)
    shift_idx = np.where(np.abs(x) != rng)[0]
    for idx in shift_idx:
        x[idx] = ((x[idx] + rng) % 360) - rng
    flip_idx = np.where(np.abs(x) == rng)[0]
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

    elif isinstance(ep_arr,
                    (float, int, np.uint32, np.int32, np.uint64, np.int64)):
        return delta(int(ep_arr), unit=unit)

    else:
        raise ValueError(
            f'input must be integer or array of integers. got {ep_arr=}{type(ep_arr)}'
        )


def delta_meters(track, rng=None):
    ''' compute distance in meters between track positions for a given track

        args:
            track (dict)
                track vector dictionary
            rng (range)
                optionally restrict computed values to given index range
    '''
    rng = range(len(track['time'])) if rng is None else rng
    return np.array(
        list(
            map(haversine, track['lon'][rng][:-1], track['lat'][rng][:-1],
                track['lon'][rng][1:], track['lat'][rng][1:])))


def delta_seconds(track, rng=None):
    ''' compute elapsed time between track positions for a given track

        args:
            track (dict)
                track vector dictionary
            rng (range)
                optionally restrict computed values to given index range
    '''
    if isinstance(track['time'], list):
        track['time'] = np.array(track['time'])
    assert isinstance(track['time'], np.ndarray), f'got {track["time"] = }'
    rng = range(len(track['time'])) if rng is None else rng
    return np.array(list((track['time'][rng][1:] - track['time'][rng][:-1])))


def delta_knots(track, rng=None):
    ''' compute speed over ground in knots between track positions for a given
        track using (distance / time)

        args:
            track (dict)
                track vector dictionary
            rng (range)
                optionally restrict computed values to given index range
    '''
    rng = range(len(track['time'])) if rng is None else rng
    ds = np.array([np.max((1, s)) for s in delta_seconds(track, rng)],
                  dtype=object)
    return delta_meters(track, rng) / ds * 1.9438445


def radial_coordinate_boundary(x, y, radius=100000):
    ''' checks maximum coordinate range for a given point and radial distance
        in meters

        args:
            x (float)
                longitude
            y (float)
                latitude
            radius (int, float)
                maximum radial distance
    '''

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
    b2 = abs(depth_metres)**2
    c2 = a2 + b2
    return np.sqrt(c2)


def vesseltrack_3D_dist(tracks, x1, y1, z1, colname='distance_metres'):
    ''' appends approximate distance to point at every position

        x1 (float)
            point longitude
        y1 (float)
            point latitude
        z1 (float)
            point depth (metres)
        colname (string)
            track dictionary key for which depth values will be set.
            by default, distances are appended to the 'distance_metres'
            key

    '''
    for track in tracks:
        track['dynamic'] = track['dynamic'].union(set([colname]))
        dists = [
            distance3D(x1=x1, y1=y1, x2=x, y2=y, depth_metres=z1)
            for x, y in zip(track['lon'], track['lat'])
        ]
        track[colname] = np.array(dists, dtype=object)
        yield track


def mask_in_radius_2D(tracks, xy, distance_meters):
    ''' radial filtering using great circle distance at surface level

        tracks (:class:`aisdb.track_gen.TrackGen`)
            track dictionary iterator
        xy (tuple of floats)
            target longitude and latitude coordinate pair
        distance_meters (int)
            maximum distance in meters
    '''
    for track in tracks:
        mask = [
            haversine(track['lon'][i], track['lat'][i], xy[0], xy[1]) <
            distance_meters for i in range(len(track['time']))
        ]
        if sum(mask) == 0:
            continue
        yield dict(
            **{k: track[k]
               for k in track['static']},
            **{k: track[k][mask]
               for k in track['dynamic']},
            static=track['static'],
            dynamic=track['dynamic'],
        )


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

        >>> domain = Domain(name='example', zones=[{
        ...     'name': 'zone1',
        ...     'geometry': shapely.geometry.Polygon([(-40,60), (-40, 61), (-41, 61), (-41, 60), (-40, 60)])
        ...     }, ])

        attr:
            self.name
            self.zones
            self.boundary
            self.minX
            self.minY
            self.maxX
            self.maxY
    '''

    def zone_max_radius(self, geom, zone_x, zone_y):
        return np.max([
            haversine(geom.centroid.x, geom.centroid.y, x2, y2)
            for x2, y2 in zip(zone_x, zone_y)
        ])

    def add_zone(self, name, x, y):
        if name[-2:] == '_b' and (x0b := np.min(x)) < self.minX_b:
            self.minX_b = x0b
        elif name[-2:] != '_c' and (x0c := np.min(x)) > self.minX:
            self.minX = x0c

        if name[-2:] == '_c' and (x1b := np.max(x)) < self.maxX_c:
            self.maxX_c = x1b
        elif name[-2:] != '_b' and (x1c := np.max(x)) > self.maxX:
            self.maxX = x1c

        if np.min(y) < self.minY:
            self.minY = np.min(y)
        if np.max(y) > self.maxY:
            self.maxY = np.max(y)

        assert -180 <= self.minX <= 180 and -180 <= self.maxX <= 180
        assert -90 <= self.minY <= 90 and -90 <= self.maxY <= 90

        geom = Polygon(zip(x, y))
        maxradius = self.zone_max_radius(geom, x, y)

        self.zones.update({name: {'geometry': geom, 'maxradius': maxradius}})

        return

    def __init__(self, name, zones=[], **kw):
        if len(zones) == 0:
            raise ValueError(
                'domain needs to have atleast one polygon geometry')
        self.name = name
        self.zones = {}
        self.minX, self.maxX = 180, -180
        self.minX_b = 180
        self.minY, self.maxY = 90, -90
        self.maxX_c = -180

        for zone in zones:
            assert 'name' in zone.keys(), f'{zone=}'
            assert 'geometry' in zone.keys(), f'{zone=}'
            x, y = zone['geometry'].boundary.coords.xy
            if not (np.min(x) >= -180 and np.max(x) <= 180):
                warnings.warn(f'dividing geometry... {zone["name"]}')
                for g in self.split_geom(zone):
                    if g.centroid.x < -180:
                        x, y = np.array(g.boundary.coords.xy)
                        self.add_zone(zone['name'] + '_b', shiftcoord(x), y)
                    elif g.centroid.x > 180:
                        x, y = np.array(g.boundary.coords.xy)
                        self.add_zone(zone['name'] + '_c', shiftcoord(x), y)
                    else:
                        x, y = np.array(g.boundary.coords.xy)
                        self.add_zone(zone['name'] + '_a', x, y)
            else:
                self.add_zone(zone['name'], x, y)

        self.boundary = {
            'xmin': self.minX,
            'xmax': self.maxX,
            'ymin': self.minY,
            'ymax': self.maxY
        }
        if self.minX_b != 180 and self.boundary['xmin'] % 180 != 0:
            assert self.minX_b >= self.boundary[
                'xmin'], f'{self.boundary=} {self.minX_b=}'
            self.boundary.update({'xmin': self.minX_b, 'xmin_alt': self.minX})
        if self.maxX_c != -180 and self.boundary['xmax'] % 180 != 0:
            assert self.maxX_c <= self.boundary[
                'xmax'], f'{self.boundary=} {self.maxX_c=}'
            self.boundary.update({'xmax': self.maxX_c, 'xmax_alt': self.maxX})

    def nearest_polygons_to_point(self, x, y):
        ''' compute great circle distance for this point to each polygon
            centroid, subtracting the maximum polygon radius.
            returns all zones with distances less than zero meters, sorted by
            nearest first
        '''
        assert float(x), f'{type(x)} {x=}'
        assert float(y), f'{type(y)} {y=}'
        assert isinstance(self.zones, dict)
        dist_to_centroids = {}
        for name, z in self.zones.items():
            dist_to_centroids.update({
                name:
                haversine(
                    x,
                    y,
                    z['geometry'].centroid.x,
                    z['geometry'].centroid.y,
                ) - z['maxradius']
            })
        return dist_to_centroids

    def point_in_polygon(self, x, y):
        ''' returns the first domain zone containing the given coordinates

            args:
                x (float)
                    longitude value
                y (float)
                    latitude value
        '''
        assert float(x), f'{type(x)} {x=}'
        assert float(y), f'{type(y)} {y=}'
        assert len(self.zones) > 0
        # first pass filter using distance to centroid, subtracting max radius.
        # discard all geometry with a distance over zero
        nearest = {
            k: v
            for k, v in sorted(self.nearest_polygons_to_point(x, y).items(),
                               key=lambda item: item[1]) if v < 0
        }
        # check for zone containment, starting with the nearest centroid
        for key, value in nearest.items():
            if self.zones[key]['geometry'].contains(Point(x, y)):
                return key
        return 'Z0'

    meridian = LineString(
        np.array((
            (-180, -180, 180, 180),
            (-90, 90, 90, -90),
        )).T)

    def split_geom(self, zone):
        merged = shapely.ops.linemerge(
            [zone['geometry'].boundary, self.meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        return decomp


class DomainFromTxts(Domain):
    ''' subclass of :class:`aisdb.gis.Domain`. used for convenience to load
        zone geometry from .txt files directly
    '''

    def __init__(self, domainName, folder, ext='txt'):

        files = glob_files(folder, ext=ext)
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
            geom = Polygon(zip(x, y))
            zones.append({'name': filename, 'geometry': geom})
        super().__init__(domainName, zones, setattrs=False)


class DomainFromPoints(Domain):
    ''' subclass of :class:`aisdb.gis.Domain`. used for convenience to load
        zone geometry from longitude and latitude pairs with a bounding-box
        maximum radial distance.
    '''

    def __init__(self,
                 points,
                 radial_distances,
                 names=[],
                 domainName='domain'):
        ''' creates a bounding-box polygons

            args:
                points (list)
                    coordinate XY pairs
                radial_distances (list)
                    approximate distance in meters to extend the bounding box.
                    the distance given will be used as the minimum distance to
                    box boundaries
                names (list)
                    optionally assign a zone name for each point


        '''
        if names == []:
            names = [f'{i:02d}' for i in range(len(points))]
        zones = []
        for xy, d, i in zip(points, radial_distances, names):
            bounds = radial_coordinate_boundary(*xy, d)
            geom = {
                'name':
                i,
                'geometry':
                Polygon([
                    (bounds['xmin'], bounds['ymin']),
                    (bounds['xmin'], bounds['ymax']),
                    (bounds['xmax'], bounds['ymax']),
                    (bounds['xmax'], bounds['ymin']),
                    (bounds['xmin'], bounds['ymin']),
                ]),
            }
            zones.append(geom)
        super().__init__(name=domainName, zones=zones)

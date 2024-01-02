'''Geometry and GIS utilities'''

import os
import pathlib
import tempfile
from datetime import datetime, timedelta
from functools import partial

import numpy as np
import shapely.ops
import shapely.geometry
import warnings
from shapely.geometry import Polygon, LineString, Point

from aisdb.aisdb import haversine
from aisdb.proc_util import glob_files


def shiftcoord(x, rng=180):
    ''' Correct longitude coordinates to be within range(-180, 180)
        using a linear shift and modulus.
        For latitude coordinate correction, set rng to 90.

        For example: longitude 181 would be corrected to -179 deg.
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
    ''' convert datetime.datetime to epoch minutes

    example:
        >>> import numpy as np
        >>> from datetime import timedelta, datetime

        >>> y1, x1 = -66.84683, -61.10595523571155
        >>> y2, x2 = -66.83036, -61.11595523571155
        >>> y3, x3 = -66.82036, -61.12595523571155
        >>> t1 = dt_2_epoch( datetime(2021, 1, 1, 1) )
        >>> t2 = dt_2_epoch( datetime(2021, 1, 1, 2) )
        >>> t3 = dt_2_epoch(datetime(2021, 1, 1, 3))

        >>> # creating a sample track
        >>> tracks_short = [
        ...    dict(
        ...        lon=np.array([x1, x2, x3]),
        ...        lat=np.array([y1, y2, y3]),
        ...        time=np.array([t1, t2, t3]),
        ...        dynamic=set(['lon', 'lat', 'time']),
        ...        static = set()
        ...    )
        ... ]

        >>> tracks__ = aisdb.interp.interp_time(tracks_short, timedelta(minutes=10))

    '''

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
    ''' compute haversine distance in meters between track positions for a
        given track

        args:
            track (dict)
                track vector dictionary
            rng (range)
                optionally restrict computed values to given index range

        Example:
            >>> import numpy as np
            >>> import aisdb
            >>> y1, x1 = -66.84683, 44.96421
            >>> y2, x2 = -66.83036, 44.9679
            >>> y3, x3 = -66.81388, 44.9716

            >>> # creating a sample track
            >>> tracks_short = [
            ...     dict( lon=np.array([x1,x2,x3]),
            ...     lat=np.array([y1,y2,y3]),
            ...     time=[0,1,2], dynamic=set(['time']), ) ]
            >>> trk_1 = tracks_short[0]
            >>> rt_ = aisdb.gis.delta_meters(trk_1)
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
        track using (haversine distance / time)

        args:
            track (dict)
                track vector dictionary
            rng (range)
                optionally restrict computed values to given index range

        Example:

            >>> import numpy as np
            >>> import aisdb
            >>> y1, x1 = -66.84683, 44.96421
            >>> y2, x2 = -66.83036, 44.9679
            >>> y3, x3 = -66.81388, 44.9716

            >>> # creating a sample track
            >>> tracks_short = [
            ...   dict(
            ...            lon=np.array([x1,x2,x3]),
            ...            lat=np.array([y1,y2,y3]),
            ...            time=[0,1,2],
            ...            dynamic=set(['time']),
            ...        )
            ... ]

            >>> trk_1 = tracks_short[0]
            >>> rt_ = aisdb.gis.delta_knots(trk_1)
    '''
    rng = range(len(track['time'])) if rng is None else rng
    ds = np.array([np.max((1, s)) for s in delta_seconds(track, rng)],
                  dtype=object)
    return delta_meters(track, rng) / ds * 1.9438445


def radial_coordinate_boundary(x, y, radius=100000):
    ''' Defines a bounding box area for a given point and radial
        distance in meters. Returns degree boundaries with a minimum diameter
        of approximately 2 * ``radius`` meters.

        The boundaries are approximated by converting input coordinates
        from degrees to radians, and computing a radial delta by
        dividing an input value by the earth radius. The radial delta
        is added or subtracted from the input point for each cardinal
        direction, and then converted back from radians to degrees.

        args:
            x (float)
                longitude
            y (float)
                latitude
            radius (int, float)
                minimum radial distance
        returns:
        dict({xmin, xmax, ymin, ymax})
    '''
    # radians
    earth_radius_m = 6371088
    rlon = np.pi * x / 180
    rlat = np.pi * y / 180
    parallel_radius = earth_radius_m * np.cos(rlat)

    # radial delta
    rlonmin = rlon - radius / parallel_radius
    rlonmax = rlon + radius / parallel_radius
    rlatmin = rlat - radius / earth_radius_m
    rlatmax = rlat + radius / earth_radius_m

    return {
        'xmin': 180 * rlonmin / np.pi,
        'xmax': 180 * rlonmax / np.pi,
        'ymin': 180 * rlatmin / np.pi,
        'ymax': 180 * rlatmax / np.pi
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
    ''' appends approximate distance to a submerged point at every
        surface-level position. distance is approximated using the haversine
        function and pythagoras.

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

    _meridian = LineString(
        np.array((
            (-180, -180, 180, 180),
            (-90, 90, 90, -90),
        )).T)

    def _zone_max_radius(self, geom, zone_x, zone_y):
        ''' computes the maximum distance to the centroid '''
        return np.max([
            haversine(geom.centroid.x, geom.centroid.y, x2, y2)
            for x2, y2 in zip(zone_x, zone_y)
        ])

    def _add_zone(self, name, x, y):
        '''
        if name[-2:] == '_b':
            if (x0b := np.min(x)) < self.minX_b:
                self.minX_b = x0b
            if (x0c := np.min(x)) > self.minX:
                self.minX = x0c

        elif name[-2:] == '_c':
            if (x1b := np.max(x)) < self.maxX_c:
                self.maxX_c = x1b
            if (x1c := np.max(x)) > self.maxX:
                self.maxX = x1c

        else:
        '''
        if ((x0a := np.min(x)) < self.minX):
            self.minX = x0a
        if ((x1a := np.max(x)) > self.maxX):
            self.maxX = x1a

        if np.min(y) < self.minY:
            self.minY = np.min(y)
        if np.max(y) > self.maxY:
            self.maxY = np.max(y)
        '''
        if np.min(x) < self.minX:
            self.minX = np.min(x)
        if np.max(x) > self.maxX:
            self.maxX = np.max(x)
        '''

        assert self.minX < self.maxX
        assert self.minY < self.maxY
        assert -180 <= self.minX <= 180 and -180 <= self.maxX <= 180
        assert -90 <= self.minY <= 90 and -90 <= self.maxY <= 90

        geom = Polygon(zip(x, y))

        self.zones.update({
            name: {
                'geometry': geom,
                'maxradius': self._zone_max_radius(geom, x, y)
            }
        })

        return

    def _handle_outofbounds_zone(self, zone, zones_dir):
        zones_west = zones_dir / 'west'
        zones_east = zones_dir / 'east'
        zones_corr = zones_dir / 'corr'
        stringify = lambda x, y: map(
            ','.join, zip(map(str, x), map(lambda y: y + '\n', map(str, y))))

        for g in self.split_geom(zone):
            if g.centroid.x < -180:
                x, y = np.array(g.boundary.coords.xy)
                if not os.path.isdir(zones_west):
                    os.mkdir(zones_west)
                with open(os.path.join(zones_west, zone['name'] + '_west.txt'),
                          'w') as w:
                    w.writelines(stringify(shiftcoord(x), y))
            elif g.centroid.x > 180:
                x, y = np.array(g.boundary.coords.xy)
                if not os.path.isdir(zones_east):
                    os.mkdir(zones_east)
                with open(os.path.join(zones_east, zone['name'] + '_east.txt'),
                          'w') as w:
                    w.writelines(stringify(shiftcoord(x), y))
            else:
                x, y = np.array(g.boundary.coords.xy)
                if not os.path.isdir(zones_corr):
                    os.mkdir(zones_corr)
                with open(os.path.join(zones_corr, zone['name'] + '_corr.txt'),
                          'w') as w:
                    w.writelines(stringify(x, y))

    def __init__(self, name, zones=[], **kw):
        ''' Initialize the domain from zone geometries '''

        if len(zones) == 0:
            raise ValueError(
                'domain needs to have atleast one polygon geometry')
        self.name = name
        self.zones = {}
        self.minX, self.maxX = 180, -180
        # self.minX_b = 180
        self.minY, self.maxY = 90, -90
        # self.maxX_c = -180

        valid_domain = True
        zones_dir = pathlib.Path(tempfile.mkdtemp(prefix='aisdb_zones_'))

        for zone in zones:
            if 'name' not in zone.keys():
                raise KeyError(f'Zone missing \'name\' key: {zone=}')
            if 'geometry' not in zone.keys():
                raise KeyError(f'Zone missing \'geometry\' key: {zone=}')

            x, y = zone['geometry'].boundary.coords.xy
            if not (np.min(x) >= -180 and np.max(x) <= 180):
                #warnings.warn(f'dividing geometry... {zone["name"]}')
                valid_domain = False
                self._handle_outofbounds_zone(zone, zones_dir)
            else:
                self._add_zone(zone['name'], x, y)

        if not valid_domain:
            '''
            if not os.path.isdir(os.path.dirname(
                    zones_corr)) or not os.path.isdir(zones_corr):
                print('Creating new output directory in ',
                      os.path.dirname(zones_dir))
                os.makedirs(zones_dir, exist_ok=True)
            '''

            for zonename, zone in self.zones.items():
                zone['name'] = zonename
                self._handle_outofbounds_zone(zone, zones_dir)

            raise ValueError(
                'Invalid zone geometry! '
                'Exceeds longitude range -180 to 180. '
                'If you want to query a bounding box spanning 180 degrees '
                'longitude, consider querying multiple times instead.\n'
                f'Saved modified geometries in {str(zones_dir)}, try using these corrected domains:\n'
                f'\tdomain1 = aisdb.DomainFromTxts(domainName=\'{name}_corr\', folder={str(zones_dir)}{os.path.sep}corrected)\n'
                f'\tdomain2 = aisdb.DomainFromTxts(domainName=\'{name}_west\', folder={str(zones_dir)}{os.path.sep}west))\n'
                f'\tdomain3 = aisdb.DomainFromTxts(domainName=\'{name}_east\', folder={str(zones_dir)}{os.path.sep}east))\n'
            )

        assert self.minX < self.maxX
        assert self.minY < self.maxY

        self.boundary = {
            'xmin': self.minX,
            'xmax': self.maxX,
            'ymin': self.minY,
            'ymax': self.maxY
        }

    def nearest_polygons_to_point(self, x, y):
        ''' compute great circle distance for this point to each polygon
            centroid, subtracting the maximum polygon radius.
            returns all zones with distances less than zero meters, sorted by
            nearest first
        '''
        assert float(x) or x == 0.0, f'{type(x)} {x=}{y=}'
        assert float(y) or y == 0.0, f'{type(y)} {x=}{y=}'
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
        ''' Returns the zone containing the given coordinates.
            if there are multiple zones containing the coordinates,
            the zone with the nearest centroid will be selected.

            args:
                x (float)
                    longitude value
                y (float)
                    latitude value
        '''
        assert float(x) or x == 0.0, f'{type(x)} {x=}{y=}'
        assert float(y) or y == 0.0, f'{type(y)} {x=}{y=}'
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

    def split_geom(self, zone):
        ''' Ensure that the zone doesn't intersect longitude 180 or -180.
            If it does, divide it into two zones.
        '''
        merged = shapely.ops.linemerge(
            [zone['geometry'].boundary, self._meridian])
        border = shapely.ops.unary_union(merged)
        decomp = shapely.ops.polygonize(border)
        return decomp


class DomainFromTxts(Domain):
    ''' subclass of :class:`aisdb.gis.Domain`. used for convenience to load
        zone geometry from .txt files directly

        example:

        >>> folder = os.path.join('aisdb/tests/test_zones')
        >>> zipf = os.path.join(folder, 'test_zones.zip')

        >>> with zipfile.ZipFile(zipf, 'r') as zip_ref:
        ...    members = list(
        ...        set(zip_ref.namelist()) - set(sorted(os.listdir(folder))))
        >>>    zip_ref.extractall(path=folder, members=members)

        >>> domain = DomainFromTxts(domainName='test', folder=folder)
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
    ''' Subclass of :class:`aisdb.gis.Domain`. Used for convenience to generate
        bounding box polygons from longitude/latitude pairs and radial
        distances, where the minimum radius is specified in meters.
    '''

    def __init__(self,
                 points,
                 radial_distances,
                 names=[],
                 domainName='domain'):
        ''' Creates bounding-box polygons having a minimum radius atleast
            approximately ``radial_distances`` in metres, centered on
            ``points``.

            args:
                points (list)
                    coordinate XY pairs
                radial_distances (list)
                    approximate distance in meters to extend the bounding box.
                    the distance given will be used as the minimum distance to
                    box boundaries
                names (list)
                    optionally assign a zone name for each point

            example:

            >>> domain = DomainFromPoints([(-45, 50), (-50, 35), (-40, 55)], [10000, 1000, 100000])


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

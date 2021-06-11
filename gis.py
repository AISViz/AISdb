import os

import numpy as np
from datetime import timedelta
from shapely.ops import unary_union
from shapely.geometry import Polygon

from track_gen import trackgen, segment, filtermask, writecsv


def haversine(x1, y1, x2, y2):
    ''' https://en.wikipedia.org/wiki/Haversine_formula '''
    x1r,y1r,x2r,y2r = map(np.radians, [x1,y1,x2,y2])
    dlon,dlat = x2r - x1r, y2r - y1r
    return 6367 * 2 * np.arcsin(np.sqrt(np.sin(dlat/2.)**2 + np.cos(y1r) * np.cos(y2r) * np.sin(dlon/2.)**2)) * 1000


def compute_knots(track, rng):#, mask=[True for _ in rng]):
    """
    diff fcn: np.abs(((track['sog'][rng][mask][:-1] + track['sog'][rng][end][1:]) / 2) - knots)
    """
    meters = np.array(list(map(haversine, track['lon'][rng][:-1], track['lat'][rng][:-1], track['lon'][rng][1:], track['lat'][rng][1:])))
    #seconds = np.array(list(map(timedelta.total_seconds, (track['time'][rng][1:] - track['time'][rng][:-1]))))
    seconds = np.array(list((track['time'][rng][1:] - track['time'][rng][:-1])))
    return meters / seconds * 1.9438445


def zones_from_txts(dirpath='../scripts/dfo_project/EastCoast_EEZ_Zones_12_8', domain='east'):
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
    

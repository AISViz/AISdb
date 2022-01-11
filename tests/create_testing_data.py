import os
import pickle
from hashlib import sha256
from functools import reduce

import numpy as np
from shapely.geometry import Polygon

from aisdb import zones_dir
from aisdb.database.lambdas import boxpoly
from aisdb.gis import shiftcoord, ZoneGeom, ZoneGeomFromTxt
from aisdb.proc_util import glob_files

arrayhash = lambda matrix, nbytes=2: sha256(
    reduce(np.append, matrix).tobytes()).hexdigest()[nbytes * -8:]


def sample_track_pickle():
    fpath = 'scripts/dfo_project/test_query_october.pickle'
    if os.path.isfile(fpath):
        with open(fpath, 'rb') as f:
            return pickle.load(f)

    maxlen = 0
    maxmmsi = ''
    tracks = {track['mmsi']: track for track in trackgen(rows)}
    for track in tracks.values():
        if (m := len(track['lon'])) > maxlen:
            maxlen = m
            maxmmsi = track['mmsi']
        print(track['mmsi'], m)

    testrows = np.array([
        [tracks[maxmmsi]['mmsi'] for _ in range(maxlen)],
        tracks[maxmmsi]['time'],
        tracks[maxmmsi]['lon'],
        tracks[maxmmsi]['lat'],
        tracks[maxmmsi]['cog'],
        tracks[maxmmsi]['sog'],
        [tracks[maxmmsi]['name'] for _ in range(maxlen)],
        [tracks[maxmmsi]['type'] for _ in range(maxlen)],
    ],
                        dtype=object).T

    with open(fpath, 'wb') as f:
        pickle.dump(testrows, f)

    return testrows


def sample_random_polygon(xscale=20, yscale=20):
    vertices = 6

    x, y = [0, 0, 0], [0, 0, 0]
    while not Polygon(zip(x, y)).is_valid:
        x = (np.random.random(vertices) * xscale) + (350 *
                                                     (np.random.random() - .5))
        y = (np.random.random(vertices) * yscale) + (170 *
                                                     (np.random.random() - .5))

    #return geom
    return x, y


def zonegeoms_or_randompoly(randomize=False, count=10):
    shapefilepaths = glob_files(zones_dir, '.txt')
    if len(shapefilepaths) > 0 and not randomize:
        zonegeoms = {
            z.name: z
            for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]
        }
    else:
        zonegeoms = {
            arrayhash(matrix): ZoneGeom(arrayhash(matrix), *matrix)
            for matrix in [sample_random_polygon() for _ in range(count)]
        }
    return zonegeoms

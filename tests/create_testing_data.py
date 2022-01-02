import pickle
from hashlib import sha256
from functools import reduce

import numpy as np
from shapely.geometry import Polygon

from aisdb.database.lambdas import boxpoly
from aisdb.gis import shiftcoord, ZoneGeom, ZoneGeomFromTxt, glob_shapetxts


arrayhash = lambda matrix, nbytes=2: sha256(reduce(np.append, matrix).tobytes()).hexdigest()[nbytes*-8:]


def sample_track_pickle():
    fpath = 'scripts/dfo_project/test_query_october.pickle'
    if os.path.isfile(fpath):
        with open (fpath, 'rb') as f:
            return pickle.load(f)

    maxlen=0
    maxmmsi=''
    tracks = {track['mmsi'] : track for track in trackgen(rows)}
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
        ], dtype=object).T


    with open(fpath, 'wb') as f:
        pickle.dump(testrows, f)

    return testrows


def sample_random_polygon():
    vertices = 5
    x,y=[0,0,0],[0,0,0]

    while not (geom := Polygon(zip(x,y))).is_valid:
        x = (np.random.random(vertices) * 5) + (360 * (np.random.random()-.5)) 
        y = (np.random.random(vertices) * 5) + (180 * (np.random.random()-.5)) 

    #return geom
    return x, y


def zonegeoms_or_randompoly(): 
    shapefilepaths = glob_shapetxts()
    if len(shapefilepaths) > 0:
        zonegeoms = {z.name : z for z in [ZoneGeomFromTxt(f) for f in shapefilepaths]} 
    else:
        print('no zone geometry found, fuzzing some new ones...')
        zonegeoms = { arrayhash(matrix) : ZoneGeom(arrayhash(matrix), *matrix) for matrix in  [sample_random_polygon() for _ in range(10)] }
    return zonegeoms


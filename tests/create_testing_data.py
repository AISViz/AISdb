import os
import pickle
from hashlib import sha256
from functools import reduce

import numpy as np
from shapely.geometry import Polygon

from aisdb import zones_dir, rawdata_dir, dbpath
from aisdb.database.sqlfcn_callbacks import boxpoly
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
    tracks = {track['mmsi']: track for track in TrackGen(rows)}
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


def create_testing_aisdata():
    with open(os.path.join(rawdata_dir, 'testingdata.nm4'), 'w') as f:
        f.write(r'''
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;I=i:8f0D4l>niTdDO`cO3jGqrlQ,0*67
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,another_error!!;wqlirf
\s:42958i,t:1635809521*6F\!AIVDM,1,1,,A,B4eIh>@0<voAFw6HKAi7swf1lH@s,0*61
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,B,14eGdb0001sM5sjIS3C5:qpt0L0G,0*0CFFF
!AIVDM,1,1,,,IjHcmoT=Jk;9uh,4*3E
!AIVDM,1,1,,,;3atgG6bSvJKGpi6=:9Twkk13W:3,0*03
!AIVDM,1,1,,,;3f?`?bDiW2w=Pt3hfnEP6pCJoli,0*4E
!AIVDM,1,1,,,;4=BV5C@NGJfs0ck@oM2gB>6E2hB,0*39
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;5L<FJ3An>wAqn=voHSOHqTv=`GN,0*21
!AIVDM,1,1,,,;7a`OobAVQO<A1nbiBc3rBqih5UB,0*78
\s:42958,c:1635809454\!AIVDM,1,1,,,;9L:cO`CgQ@S:NcT04HENVk@:JR=,0*5F
!AIVDM,1,1,,,;:JvB;MhC4pvK3KB43F60v4bAhuF,0*7B
,t:1635809521*6F!AIVDM,1,1,,,;;d6bbCsM8qH5>?=U0BMdo>>VvmU,0*39
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;;si?Qj0:wNL4tDTd`BN41nL0D11,0*0D
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;=K;HJ:wsf0Bg8IDJ2MQ7PISJ;jJ,0*23
\c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;=OeV7HR4n8tM3grUTk1Cs9glLGE,0*5A
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;@Ha?G0t<>ekGDOI:>sE<2BnWHNr,0*33
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;AHld`?P<wLu6<T:L6TVm0QqcQWl,0*48
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;BI:gwCuqWqP7Wr8JVKTwAIDRiWl,0*5E
\s:42958,t:1635809521*6F\!AIVDM,1,1,,,;EH8O`wtiWs;0MmE@F;U2:srnf?E,0*75
\s:42958,c:,t:1635809521*6F\!AIVDM,1,1,,,;I=i:8f0D4l>niTdDO`cO3jGqrlQ,0*67
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;KaC759LogaaW=4r:nn>VEc<m2qs,0*73
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;KpNTNJNbq9wMffET:P<C35Pmo`1,0*26
\g:23412341234kj\!AIVDM,1,1,,,;M`m7tluWVNmIBnh5NoiARj<spps,0*51
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;OJvuN<7esIlAgPIus4NJa:UlqDP,0*22
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;RIRt:dp:3qqmr67hRoGGJ>e7uTi,0*63
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;U>f0=vU6au?gW@E8UuVoI=P07H=,0*20
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;cvCpIRTKPSqU9kT0dOLKWuC2KE2,0*2C
\s:42958,c:-12134,t:1635809521*6F\!AIVDM,1,1,,,;cw3<IPo<pHlaoEPuT9PqcAn5fnM,0*47
\s:42958,c:asbhdjf,t:1635809521*6F\!AIVDM,1,1,,,;eeA1PBssU1OQwN8orvatv97;@tm,0*21
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;g9tT:6O@0Ujsr<mCJCwnAG83cv?,0*0A
\s:42958,c1635809454,t:1635809521*6F\!AIVDM,1,1,,,;h3woll47wk?0<tSF0l4Q@000P00,0*4B
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,1,1,,,;i<rac5sg@;huMi4QhiWacTLEQj<,0*71
\s:42958,c:1635809454,t:1635809521*6F\!AIVDM,2,1,4,,54sc8041SAu`uDPr220dU<56222222222222221618@247?m07SUEBp1,0*0C
\c:1617253215*5A\!AIVDM,1,1,,,13nWPR0003K7<OsQsrGW1K>L0881,0*68
\c:1617284347*56\!AIVDM,1,1,,,13n7aN0wQnsN4lfE8nEUgDf:0<00,0*18
\c:1617289692*56\!AIVDM,1,1,,,C4N6S1005=6h:aw8::=9CwTHL:`<BVAWWWKQa1111110CP81110W,0*7A'''
                )

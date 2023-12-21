import os
from datetime import datetime

import aisdb
from aisdb import DBQuery, DBConn
from aisdb.gis import DomainFromTxts, Domain
import shapely
from dotenv import load_dotenv
from aisdb.tests.test_08_marinetraffic import testdir
load_dotenv()

dbpath = os.environ.get('EXAMPLE_NOISE_DB', 'AIS.sqlitedb')

trafficDBpath = os.path.join(testdir, 'marinetraffic_test.db')
# trafficDBpath = os.environ.get('AISDBMARINETRAFFIC', 'marinetraffic.db')
domain = domain = Domain(name='example', zones=[{'name': 'zone1',
        'geometry': shapely.geometry.Polygon([(-40,60), (-40, 61), (-41, 61), (-41, 60), (-40, 60)])
        }, ]) #DomainFromTxts('EastCoast', folder=os.environ.get('AISDBZONES'))

start = datetime(2021, 7, 1)
end = datetime(2021, 7, 2)

default_boundary = {'xmin': -180, 'xmax': 180, 'ymin': -90, 'ymax': 90}


def random_noise(tracks, boundary=default_boundary):
    for track in tracks:
        i = 1
        while i < len(track['time']):
            track['lon'][i] *= track['mmsi']
            track['lon'][i] %= (boundary['xmax'] - boundary['xmin'])
            track['lon'][i] += boundary['xmin']
            track['lat'][i] *= track['mmsi']
            track['lat'][i] %= (boundary['ymax'] - boundary['ymin'])
            track['lat'][i] += boundary['ymin']
            i += 2
        yield track


with DBConn(dbpath) as dbconn:
    vinfoDB = aisdb.webdata.marinetraffic.VesselInfo(trafficDBpath).trafficDB

    qry = DBQuery(
        dbconn=dbconn,
        dbpath=dbpath,
        start=start,
        end=end,
        callback=aisdb.database.sqlfcn_callbacks.in_bbox_time_validmmsi,
        **domain.boundary,
    )

    rowgen = qry.gen_qry(fcn=aisdb.database.sqlfcn.crawl_dynamic_static)

    tracks = aisdb.track_gen.TrackGen(rowgen, decimate=True)
    tracks = aisdb.webdata.marinetraffic.vessel_info(tracks, vinfoDB)
    tracks = random_noise(tracks, boundary=domain.boundary)
    tracks = aisdb.encode_greatcircledistance(tracks,
                                              distance_threshold=50000,
                                              minscore=1e-5,
                                              speed_threshold=50)

    if __name__ == '__main__':
        aisdb.web_interface.visualize(
            tracks,
            domain=domain,
            visualearth=True,
            open_browser=True,
        )

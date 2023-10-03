import os
from datetime import datetime, timedelta

import warnings

from aisdb import track_gen, DBQuery, sqlfcn_callbacks, DBConn
from aisdb import encode_greatcircledistance
from aisdb.wsa import wetted_surface_area
from aisdb.database import sqlfcn
from aisdb.webdata.marinetraffic import vessel_info, VesselInfo

from aisdb.tests.create_testing_data import sample_database_file

testdir = os.environ.get(
    'AISDBTESTDIR',
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        'testdata',
    ),
)
if not os.path.isdir(testdir):
    os.mkdir(testdir)

trafficDBpath = os.path.join(testdir, 'marinetraffic_test.db')


def test_wetted_surface_area_regression_marinetraffic(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)
    vinfoDB = VesselInfo(trafficDBpath).trafficDB
    with DBConn(
            dbpath) as dbconn, warnings.catch_warnings(), vinfoDB as trafficDB:
        warnings.simplefilter('ignore')

        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(fcn=sqlfcn.crawl_dynamic_static, verbose=True)
        tracks = vessel_info(
            encode_greatcircledistance(
                track_gen.TrackGen(rowgen, decimate=True),
                distance_threshold=250000,
            ),
            dbconn=trafficDB,
        )

        for track in tracks:
            track = next(wetted_surface_area([track]))

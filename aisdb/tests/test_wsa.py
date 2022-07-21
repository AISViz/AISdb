import os
from datetime import datetime
import pickle

from aisdb import track_gen, decode_msgs, DBQuery, sqlfcn_callbacks, DBConn
from aisdb.track_gen import encode_greatcircledistance
#from aisdb.webdata.marinetraffic import vessel_info
from aisdb.wsa import wetted_surface_area
from aisdb.database import sqlfcn

start = datetime(2021, 11, 1)
end = datetime(2021, 11, 2)


def test_wetted_surface_area_regression(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    datapath = os.path.join(os.path.dirname(__file__),
                            'testingdata_20211101.nm4')
    testingdata = os.path.join(os.path.dirname(__file__),
                               'test_wsa_shipmetadata.pickle')
    with DBConn(dbpath=dbpath) as db:
        decode_msgs(
            db=db,
            filepaths=[datapath],
            dbpath=dbpath,
            source='TESTING',
            vacuum=False,
            skip_checksum=True,
        )

        qry = DBQuery(
            db=db,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_timerange_validmmsi,
        )
        rowgen = qry.gen_qry(dbpath,
                             fcn=sqlfcn.crawl_dynamic_static,
                             printqry=True)
        tracks = encode_greatcircledistance(
            track_gen.TrackGen(rowgen),
            distance_threshold=250000,
        )
        with open(testingdata, 'rb') as f:
            output_shiptypes = pickle.load(f)

        for track in tracks:
            track['marinetraffic_info'] = output_shiptypes[track['mmsi']]
            track = next(wetted_surface_area([track]))

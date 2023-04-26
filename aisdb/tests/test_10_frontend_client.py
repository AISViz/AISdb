from datetime import datetime, timedelta
import os

import aisdb
import aisdb.web_interface
from aisdb.tests.create_testing_data import sample_database_file

import numpy as np


#db_hostname = os.environ.get('AISDBWEBSOCKET', 'wss://aisdb.meridian.cs.dal.ca/ws')

# the default address used for a local docker install
# db_hostname = 'ws://[fc00::6]:9924'



def test_frontend(tmpdir):
    dbpath = os.path.join(tmpdir, 'test_trackgen.db')
    months = sample_database_file(dbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=4)

    with aisdb.DBConn() as dbconn:
        qry = aisdb.DBQuery(
                dbconn=dbconn,
                dbpath=dbpath,
                start=start,
                end=end,
                callback=aisdb.sqlfcn_callbacks.valid_mmsi,
                )
        rowgen = qry.gen_qry(verbose=True)
        tracks = aisdb.track_gen.TrackGen(rowgen, decimate=True)

        '''
        for track in tracks:
            assert 'time' in track.keys()
            if len(track['time']) >= 3:
                print(track)
            assert isinstance(track['lon'], np.ndarray)
            assert isinstance(track['lat'], np.ndarray)
            assert isinstance(track['time'], np.ndarray)
            print(track)
        '''
        aisdb.web_interface.serve_tracks(tracks)



if __name__ == '__main__':

    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        test_frontend(tmpdir)

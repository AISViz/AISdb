from datetime import datetime, timedelta
import os
import tempfile

import aisdb
import aisdb.web_interface
from aisdb.tests.create_testing_data import sample_database_file

# Ensure the web application front end is running before proceeding!

with tempfile.TemporaryDirectory() as tmpdir:
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
        rowgen = qry.gen_qry(fcn=aisdb.database.sqlfcn.crawl_dynamic_static,
                             verbose=False)
        tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)
        aisdb.web_interface.visualize(tracks)

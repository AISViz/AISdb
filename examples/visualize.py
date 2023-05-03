from datetime import datetime, timedelta
import os
import tempfile

import aisdb
import aisdb.web_interface
from aisdb.tests.create_testing_data import sample_database_file, random_polygons_domain

domain = random_polygons_domain()

example_dir = 'testdata'
if not os.path.isdir(example_dir):
    os.mkdir(example_dir)
dbpath = os.path.join(example_dir, 'example_visualize.db')
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
    rowgen = qry.gen_qry(fcn=aisdb.database.sqlfcn.crawl_dynamic_static, verbose=False)
    tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)

    if __name__ == '__main__':
        aisdb.web_interface.visualize(tracks, domain=domain, visualearth=True, open_browser=True)

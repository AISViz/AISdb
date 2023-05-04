import os
from datetime import datetime, timedelta

import aisdb
import aisdb.web_interface
from aisdb.tests.create_testing_data import (
    sample_database_file,
    random_polygons_domain,
)

domain = random_polygons_domain()

example_dir = 'testdata'
if not os.path.isdir(example_dir):
    os.mkdir(example_dir)

dbpath = os.path.join(example_dir, 'example_visualize.db')
months = sample_database_file(dbpath)
start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
end = datetime(int(months[1][0:4]), int(months[1][4:6]) + 1, 1)

with aisdb.SQLiteDBConn() as dbconn:
    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath=dbpath,
        start=start,
        end=end,
        callback=aisdb.sqlfcn_callbacks.valid_mmsi,
    )
    rowgen = qry.gen_qry()
    tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)
    tracks = aisdb.track_gen.split_timedelta(tracks, timedelta(weeks=4))

    if __name__ == '__main__':
        aisdb.web_interface.visualize(
            tracks,
            domain=domain,
            visualearth=True,
            open_browser=True,
        )

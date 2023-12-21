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


def color_tracks(tracks):
    ''' set the color of each vessel track using a color name or RGB value '''
    for track in tracks:
        track['color'] = 'red' or 'rgb(255,0,0)'
        yield track


with aisdb.SQLiteDBConn(dbpath) as dbconn:
    qry = aisdb.DBQuery(
        dbconn=dbconn,
        dbpath=dbpath,
        start=start,
        end=end,
        callback=aisdb.sqlfcn_callbacks.valid_mmsi,
    )
    rowgen = qry.gen_qry()
    tracks = aisdb.track_gen.TrackGen(rowgen, decimate=False)
    tracks_segment = aisdb.track_gen.split_timedelta(tracks,
                                                     timedelta(weeks=4))
    tracks_colored = color_tracks(tracks_segment)

    if __name__ == '__main__':
        aisdb.web_interface.visualize(
            tracks_colored,
            domain=domain,
            visualearth=True,
            open_browser=True,
        )

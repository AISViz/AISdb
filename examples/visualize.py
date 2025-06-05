import os
from datetime import datetime, timedelta

import aisdb.web_interface
from aisdb.tests.create_testing_data import (
    sample_database_file,
    random_polygons_domain,
)
from aisdb import PostgresDBConn, DBQuery, sqlfcn_callbacks, track_gen

start = datetime(2023, 12, 1)
end = datetime(2024, 1, 1)

conn_information = (f"postgresql://{os.environ['pguser']}:{os.environ['pgpass']}@"
                    f"{os.environ['pghost']}:5432/{os.environ['pguser']}")

domain = random_polygons_domain()

def color_tracks(tracks, color='yellow'):
    ''' set the color of each vessel track using a color name or RGB value '''
    for track in tracks:
        track['color'] = color
        yield track


with PostgresDBConn(conn_information) as dbconn:
    qry = DBQuery(
        dbconn=dbconn,
        start=start,
        end=end,
        callback=sqlfcn_callbacks.valid_mmsi,
    )
    rowgen = qry.gen_qry()
    tracks = track_gen.TrackGen(rowgen, decimate=False)
    tracks_segment = track_gen.split_timedelta(tracks, timedelta(weeks=4))
    tracks_colored = color_tracks(tracks_segment)

    if __name__ == '__main__':
        aisdb.web_interface.visualize(
            tracks_colored,
            domain=domain,
            visualearth=True,
            open_browser=True,
        )

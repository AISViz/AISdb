import os
import warnings
from datetime import datetime, timedelta

from shapely.geometry import Polygon
from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import PostgresDBConn
from aisdb.database.dbqry import DBQuery
from aisdb.gis import Domain
from aisdb.network_graph import graph
from aisdb.tests.create_testing_data import sample_database_file, sample_gulfstlawrence_bbox

POSTGRES_CONN_STRING = (f"postgresql://{os.environ['pguser']}:{os.environ['pgpass']}@"
                    f"{os.environ['pghost']}:5432/{os.environ['pguser']}")

# Set data_dir to your testdata folder
data_dir = os.environ.get(
    "AISDBDATADIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "testdata"),
)

# Domain polygons
lon, lat = sample_gulfstlawrence_bbox()
z1 = Polygon(zip(lon, lat))
z2 = Polygon(zip(lon - 45, lat))
z3 = Polygon(zip(lon, lat - 45))


def test_graph_minimal(tmpdir):
    domain = Domain(
        "gulf domain",
        zones=[
            {"name": "z1", "geometry": z1},
            {"name": "z2", "geometry": z2},
            {"name": "z3", "geometry": z3},
        ],
    )

    # ✅ Setup test data in PostgreSQL
    months = sample_database_file(POSTGRES_CONN_STRING)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=1)

    outputfile = os.path.join(tmpdir, "output.csv")

    with PostgresDBConn(POSTGRES_CONN_STRING) as dbconn:
        qry = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            callback=sqlfcn_callbacks.in_bbox,
            fcn=sqlfcn.crawl_dynamic_static,
            **domain.boundary,
        )

        print(f"raw count: {len(list(qry.gen_qry()))}")

        graph(
            qry,
            outputfile=outputfile,
            data_dir=data_dir,
            dbconn=dbconn,
            domain=domain,
        )

    if os.path.isfile(outputfile):
        os.remove(outputfile)
    else:
        warnings.warn("No output file generated for test_graph")

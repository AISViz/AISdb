import os
import warnings
from datetime import datetime, timedelta

from shapely.geometry import Polygon

from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database.dbqry import DBQuery
from aisdb.gis import Domain
from aisdb.network_graph import graph
from aisdb.tests.create_testing_data import (sample_database_file, sample_gulfstlawrence_bbox, )

trafficDBpath = os.environ.get("AISDBMARINETRAFFIC",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "testdata", "marinetraffic_test.db", ))

data_dir = os.environ.get("AISDBDATADIR",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "testdata", ), )

lon, lat = sample_gulfstlawrence_bbox()
z1 = Polygon(zip(lon, lat))
z2 = Polygon(zip(lon - 45, lat))
z3 = Polygon(zip(lon, lat - 45))


def test_graph_minimal(tmpdir):
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": z1}, {"name": "z2", "geometry": z2},
        {"name": "z3", "geometry": z3}, ])

    testdbpath = os.path.join(tmpdir, "test_graph_minimal.db")

    months = sample_database_file(testdbpath)
    start = datetime(int(months[0][0:4]), int(months[0][4:6]), 1)
    end = start + timedelta(weeks=1)

    with DBConn(testdbpath) as ais_database:
        qry = DBQuery(dbconn=ais_database, start=start, end=end, callback=sqlfcn_callbacks.in_bbox,
                      fcn=sqlfcn.crawl_dynamic_static, **domain.boundary)

        outputfile = os.path.join(tmpdir, "output.csv")
        print(f"raw count: {len(list(qry.gen_qry()))}")

        graph(qry, outputfile=outputfile, data_dir=data_dir, dbconn=ais_database, domain=domain,
              trafficDBpath=trafficDBpath)
    ais_database.close()
    if os.path.isfile(outputfile):
        os.remove(outputfile)
    else:
        warnings.warn("no output file generated for test graph")
    # os.remove(testdbpath)

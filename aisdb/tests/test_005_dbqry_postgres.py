import os
from datetime import datetime, timedelta
from shapely.geometry import Polygon
import urllib
import warnings

from aisdb import (PostgresDBConn, DBQuery, Domain, sqlfcn, sqlfcn_callbacks)
from aisdb.tests.create_testing_data import sample_gulfstlawrence_bbox


# PostgreSQL DB connection string from environment
def get_pg_conn_string():
    return (
        f"postgresql://{os.environ['pguser']}:{os.environ['pgpass']}@"
        f"{os.environ['pghost']}:5432/{os.environ['pguser']}"
    )

def test_query_emptytable_postgres():
    warnings.filterwarnings("error")
    conn_str = get_pg_conn_string()
    start = datetime(2021, 1, 1)
    end = datetime(2021, 1, 7)
    try:
        with PostgresDBConn(conn_str) as dbconn:
            q = DBQuery(
                dbconn=dbconn,
                start=start,
                end=end,
                callback=sqlfcn_callbacks.in_timerange_validmmsi
            )
            rows = q.gen_qry(reaggregate_static=True)
            assert list(rows) == []
    except UserWarning as warn:
        assert "No static data for selected time range!" in warn.args[0]
    except Exception as err:
        raise err

def test_prepare_qry_domain_postgres():
    conn_str = get_pg_conn_string()
    start = datetime(2021, 5, 1)
    end = start + timedelta(weeks=4)
    box_x, box_y = sample_gulfstlawrence_bbox()
    polygon = Polygon(zip([box_x[0], box_x[1], box_x[1], box_x[0]],
                          [box_y[0], box_y[0], box_y[1], box_y[1]]))
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": polygon}])
    
    with PostgresDBConn(conn_str) as dbconn:
        rowgen = DBQuery(
            dbconn=dbconn,
            start=start,
            end=end,
            **domain.boundary,
            callback=sqlfcn_callbacks.in_timerange
        ).gen_qry(reaggregate_static=True)
        try:
            next(rowgen)
        except StopIteration:
            pass


def test_sql_query_strings_postgres():
    conn_str = get_pg_conn_string()
    start = datetime(2021, 5, 1)
    end = start + timedelta(weeks=4)
    box_x, box_y = sample_gulfstlawrence_bbox()
    polygon = Polygon(zip([box_x[0], box_x[1], box_x[1], box_x[0]],
                          [box_y[0], box_y[0], box_y[1], box_y[1]]))
    domain = Domain("gulf domain", zones=[{"name": "z1", "geometry": polygon}])
    
    callbacks = [
        sqlfcn_callbacks.in_bbox_geom,
        sqlfcn_callbacks.in_bbox_time_geom,
        sqlfcn_callbacks.in_bbox_time_validmmsi_geom,
        sqlfcn_callbacks.in_time_bbox_inmmsi_geom,
        sqlfcn_callbacks.in_timerange,
        sqlfcn_callbacks.in_timerange_hasmmsi,
        sqlfcn_callbacks.in_timerange_validmmsi,
    ]
    
    with PostgresDBConn(conn_str) as dbconn:
        for callback in callbacks:
            print(f"\n--- Testing {callback.__name__} ---")
            rowgen = DBQuery(
                dbconn=dbconn,
                start=start,
                end=end,
                **domain.boundary,
                callback=callback,
                mmsi=316000000,
                mmsis=[316000000, 316000001],
            ).gen_qry(fcn=sqlfcn.crawl_dynamic_static)
            
            try:
                result = next(rowgen)
                assert isinstance(result, list)
                print(f"✔ {len(result)} rows returned")
            except StopIteration:
                print("⚠ No results for this callback")
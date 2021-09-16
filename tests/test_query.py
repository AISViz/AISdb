from datetime import datetime, timedelta

import numpy as np
#np.set_printoptions(precision=5, linewidth=80, formatter=dict(datetime=datetime, timedelta=timedelta), floatmode='maxprec', suppress=True)
import shapely.wkt
from shapely.geometry import Polygon, LineString, MultiPoint

from database import *
from gis import *
from track_gen import *


#canvaspoly = viz.poly_from_coords()  # select map coordinates with the cursor
canvaspoly = shapely.wkt.loads( 'POLYGON ((-61.51747881355931 46.25069648888631, -62.00013241525424 46.13520233725761, -62.19676906779659 45.77895246569407, -61.8452065677966 45.27803122330256, -61.56514830508475 45.10586058602501, -60.99907309322032 45.05537064981205, -60.71305614406779 45.20670660550304, -60.46875 45.56660601402942, -60.85010593220338 45.86615507310925, -61.13016419491525 45.92006919377324, -61.51747881355931 46.25069648888631))')


start = datetime(2020,9,1)
end = datetime(2020,10,1)


def test_query_smallboundary_statictables():

    start   = datetime(2018,6,1)
    end     = datetime(2018,6,2)

    # static: msg 5 union 24
    dt = datetime.now()
    rows = qrygen(
            start=start,
            end=end,
        ).run_qry(dbpath=dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=static) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')



def test_query_smallboundary_dynamictables():
    start   = datetime(2018,6,1)
    end     = datetime(2018,6,2)

    # dynamic: msg 123 union 18
    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start=start,
            end=end,
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath=dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=rtree_dynamic) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')


def test_query_smallboundary_join_static_dynamic_rtree_in_bbox_mmsi_time():

    # join rtree tables with aggregate position reports 
    start   = datetime(2018,6,1)
    end     = datetime(2018,6,2)

    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            end     = end,
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=leftjoin_dynamic_static)
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

    '''
    
    aisdb = dbconn(dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    cur.execute('select * from static_202009_aggregate where mmsi = 316020160')
    cur.execute('select * from static_202009_aggregate ')
    res = np.array(cur.fetchall())

    '''


def test_query_join_static_dynamic_rtree_in_bbox():

    # join rtree tables with aggregate position reports 
    start   = datetime(2018,6,1)
    end     = datetime(2018,7,1)

    dt = datetime.now()
    rows = qrygen(
            #xy = merge(canvaspoly.boundary.coords.xy),
            start   = start,
            end     = end,
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath, callback=rtree_in_bbox, qryfcn=leftjoin_dynamic_static)
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')


def test_drop_intermediate_tables():

    aisdb = dbconn(dbpath=dbpath, postgres=False)
    conn, cur = aisdb.conn, aisdb.cur

    mstr = start.strftime('%Y%m')
    qry = f'drop table ais_{mstr}_msg_1_2_3'
    cur.execute(qry)
    qry = f'drop table ais_{mstr}_msg_18'
    cur.execute(qry)
    qry = f'drop table ais_{mstr}_msg_5'
    cur.execute(qry)
    qry = f'drop table ais_{mstr}_msg_24'
    cur.execute(qry)
    qry = f'vacuum into "/meridian/aisdb/vacuumed_202009.db" '
    cur.execute(qry)



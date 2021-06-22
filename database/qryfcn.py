from database.qryfcn_legacy import *


rtree_nogeom = lambda month, callback, kwargs, bounds=dict(xmin=-180, ymin=-90, xmax=180, ymax=90): (f'''
SELECT m123.mmsi0, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON m123.mmsi0 = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m123.x0 >= {kwargs['xmin']}
  AND m123.x1 <= {kwargs['xmax']}
  AND m123.y0 >= {kwargs['ymin']}
  AND m123.y1 <= {kwargs['ymax']}
  AND {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi0, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON m18.mmsi0 = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m18.x0 >= {kwargs['xmin']}
  AND m18.x1 <= {kwargs['xmax']}
  AND m18.y0 >= {kwargs['ymin']}
  AND m18.y1 <= {kwargs['ymax']}
  AND {callback(month=month, alias='m18', **kwargs)}
ORDER BY 1, 2
  ''')


rtree_minified = lambda month, callback, kwargs: (f'''
SELECT CAST(m123.mmsi0 AS INT), m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON CAST(m123.mmsi0 AS INT) = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT CAST(m18.mmsi0 AS INT), m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON CAST(m18.mmsi0 AS INT) = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m18', **kwargs)}
--ORDER BY 1, 2
  ''')



# query a union of position reports using rtree indexes
rtree_dynamic = lambda month, callback, kwargs: (f'''
    SELECT CAST(m123.mmsi0 AS INT) as mmsi, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, '123' as msgtype
      FROM rtree_{month}_msg_1_2_3 AS m123
      WHERE {callback(month=month, alias='m123', **kwargs)}
    UNION
    SELECT CAST(m18.mmsi0 AS INT) as mmsi, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, '18' as msgtype
      FROM rtree_{month}_msg_18 AS m18
      WHERE {callback(month=month, alias='m18', **kwargs)} 
''')


# static table views are generated at query-time using CREATE TABLE IF NOT EXISTS... 
# see build_views function in qrygen.py 
static = lambda month, **_: (f'''
    SELECT mmsi, vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star FROM view_{month}_static ''')


leftjoin_dynamic_static = lambda month, callback, kwargs: (f'''
WITH dynamic AS MATERIALIZED ( {rtree_dynamic(month, callback, kwargs)} 
),
static AS MATERIALIZED ( {static(month)} 
)
SELECT dynamic.mmsi, dynamic.t0, dynamic.x0, dynamic.y0, dynamic.cog, dynamic.sog, static.vessel_name, ref.coarse_type_txt, dynamic.msgtype
    FROM dynamic LEFT JOIN static
      ON dynamic.mmsi = static.mmsi
    LEFT JOIN coarsetype_ref AS ref 
      ON (static.ship_type = ref.coarse_type) ''')



if False:  # testing 

    from datetime import datetime, timedelta
    import numpy as np
    np.set_printoptions(precision=5, linewidth=80, formatter=dict(datetime=datetime, timedelta=timedelta), floatmode='maxprec', suppress=True)
    import shapely.wkt

    from database import *


    dbpath = '/run/media/matt/My Passport/june2018_vacuum.db'

    #canvaspoly = shapely.wkt.loads( 'POLYGON ((-61.51747881355931 46.25069648888631, -62.00013241525424 46.13520233725761, -62.19676906779659 45.77895246569407, -61.8452065677966 45.27803122330256, -61.56514830508475 45.10586058602501, -60.99907309322032 45.05537064981205, -60.71305614406779 45.20670660550304, -60.46875 45.56660601402942, -60.85010593220338 45.86615507310925, -61.13016419491525 45.92006919377324, -61.51747881355931 46.25069648888631))')
    canvaspoly = viz.poly_from_coords()  # select map coordinates with the cursor
    poly_xy = canvaspoly.boundary.coords.xy


    # dynamic: msg 123 union 18
    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,2),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath=dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=rtree_dynamic) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

    # static: msg 5 union 24
    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,2),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath=dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=static) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')

    # msg 5 union 24
    dt = datetime.now()
    rows = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,2),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).run_qry(dbpath, callback=rtree_in_bbox_time_mmsi, qryfcn=leftjoin_dynamic_static) 
    delta =datetime.now() - dt
    print(f'query time: {delta.total_seconds():.2f}s')


    month = '201806'
    aisdb = dbconn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_shiptype ON 'ais_{month}_msg_5' (ship_type) ''')
    cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_vesselname ON 'ais_{month}_msg_5' (vessel_name) ''')
    conn.close()


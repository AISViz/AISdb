
from database import *

def test_sdd_hdd():

    # load in some data
    fpath   = '/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-06-01.csv'
    dbpath1 = 'output/dbtest1.db'
    dbpath2 = '/run/media/matt/My Passport/dbtest2.db'
    
    # test parsing time
    t0 = datetime.now()
    decode_raw_pyais(fpath, dbpath1)
    t1 = datetime.now()
    print(f'dbpath1: {dbpath1}\t{(t1-t0).total_seconds()}s')

    t2 = datetime.now()
    decode_raw_pyais(fpath, dbpath2)
    t3 = datetime.now()
    print(f'dbpath2: {dbpath2}\t{(t3-t2).total_seconds()}s')

    import shapely.wkt
    canvaspoly = shapely.wkt.loads( 'POLYGON ((-61.51747881355931 46.25069648888631, -62.00013241525424 46.13520233725761, -62.19676906779659 45.77895246569407, -61.8452065677966 45.27803122330256, -61.56514830508475 45.10586058602501, -60.99907309322032 45.05537064981205, -60.71305614406779 45.20670660550304, -60.46875 45.56660601402942, -60.85010593220338 45.86615507310925, -61.13016419491525 45.92006919377324, -61.51747881355931 46.25069648888631))')
    poly_xy = canvaspoly.boundary.coords.xy
    
    # test query time
    aisdb = dbconn(dbpath=dbpath1)
    conn, cur = aisdb.conn, aisdb.cur
    qry = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,2),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).crawl(callback=rtree_in_bbox_time_mmsi, qryfcn=rtree_minified) 
    dt = datetime.now()
    cur.execute(qry)
    delta =datetime.now()
    rows = np.array(cur.fetchall())
    print(f'query time {dbpath1}: {(delta - dt).microseconds}s')
    conn.close()

    aisdb = dbconn(dbpath=dbpath2)
    conn, cur = aisdb.conn, aisdb.cur
    qry = qrygen(
            xy = merge(canvaspoly.boundary.coords.xy),
            start   = datetime(2018,6,1),
            end     = datetime(2018,6,2),
            xmin    = min(poly_xy[0]), 
            xmax    = max(poly_xy[0]), 
            ymin    = min(poly_xy[1]), 
            ymax    = max(poly_xy[1]),
        ).crawl(callback=rtree_in_bbox_time_mmsi, qryfcn=rtree_minified) 
    dt = datetime.now()
    cur.execute(qry)
    delta =datetime.now()
    rows = np.array(cur.fetchall())
    print(f'query time {dbpath1}: {(delta - dt).microseconds}s')
    conn.close()

    #os.remove(dbpath1)
    #os.remove(dbpath2)

os.path.listdir(dbpath1)


def test_query_filter_():

    month = '201806'

    callback    = rtree_in_bbox_time_mmsi
    qryfcn      = rtree_minified

    dbpath = 'output/ais_2018-03-28.db'

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

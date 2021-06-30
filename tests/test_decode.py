import os

from database import *


"""
month = '201806'
aisdb = dbconn(dbpath=dbpath)
conn, cur = aisdb.conn, aisdb.cur
cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_shiptype ON 'ais_{month}_msg_5' (ship_type) ''')
cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_vesselname ON 'ais_{month}_msg_5' (vessel_name) ''')
conn.close()
"""




def test_parse_24h():
    dbpath = '/run/media/matt/My Passport/june2018-06-01_test.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018'))[0], dtype=object)
    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2018-06' in f])

    dt = datetime.now()
    parallel_decode([filepaths[0]], dbpath)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_parse_10d():
    dbpath = '/run/media/matt/My Passport/june2018-06-0_test2.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018'))[0], dtype=object)

    june = np.array(sorted(filenames))[ ['2018-06-0' in filename for filename in sorted(filenames)] ]

    for filename in june:
        decode_raw_pyais(fpath=os.path.join(dirpath, filename), dbpath=dbpath)
    

def test_parse_1m():
    dbpath = '/run/media/matt/My Passport/june2018-06_test3.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018'))[0], dtype=object)

    june = np.array(sorted(filenames))[ ['2018-06' in filename for filename in sorted(filenames)] ]

    for filename in june:
        decode_raw_pyais(fpath=os.path.join(dirpath, filename), dbpath=dbpath)


def test_parse_1m_eE():
    dbpath = '/meridian/aisdb/eE_202009_test2.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/meridian/AIS_archive/meopar/2020/202009'))[0], dtype=object)

    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '.nm4' in f])

    dt = datetime.now()
    parallel_decode(filepaths, dbpath)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_sort_1w():
    from database import *
    dbpath = '/run/media/matt/My Passport/june2018-06-01_test.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018'))[0], dtype=object)
    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2018-06' in f])
    filepaths = filepaths[0:7]
    dt = datetime.now()
    parallel_decode(filepaths, dbpath)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_sort_1m():
    from database import *
    dbpath = '/run/media/matt/My Passport/201806_test_paralleldecode.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018'))[0], dtype=object)
    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2018-06' in f])
    dt = datetime.now()
    parallel_decode(filepaths, dbpath)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_aggregate_staticreports():

    aisdb = dbconn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    month = '201806'

    cur.execute(f''' DROP TABLE IF EXISTS static_{month}_aggregate ''')

    agg_rows = []

    cur.execute(f"""
        SELECT DISTINCT m5.mmsi
          FROM ais_{month}_msg_5 AS m5
         UNION 
        SELECT DISTINCT m24.mmsi
          FROM ais_{month}_msg_24 AS m24
          ORDER BY 1
    """)
    mmsis = np.array(cur.fetchall(), dtype=object).flatten()

    from collections import Counter

    for mmsi in mmsis :
        cur.execute(f"""
        SELECT m5.mmsi, m5.vessel_name, m5.ship_type, m5.dim_bow, m5.dim_stern, 
            m5.dim_port, m5.dim_star, m5.imo
          FROM ais_{month}_msg_5 AS m5
          WHERE m5.mmsi = ?
          UNION ALL
        SELECT m24.mmsi, m24.vessel_name, m24.ship_type, m24.dim_bow, m24.dim_stern, 
               m24.dim_port, m24.dim_star, NULL as imo
          FROM ais_{month}_msg_24 AS m24
          WHERE m24.mmsi = ?
        """, [mmsi, mmsi])
        cols = np.array(cur.fetchall(), dtype=object).T
        filtercols = np.array([np.array(list(filter(None, col)), dtype=object) for col in cols ], dtype=object)
        paddedcols = np.array([col if len(col) > 0 else [None] for col in filtercols])
        aggregated =  [Counter(cols[i]).most_common(1)[0][0] for i in range(len(cols))]
        agg_rows.append(aggregated)

    cur.execute(f''' 
        CREATE TABLE static_{month}_aggregate (
            mmsi INTEGER PRIMARY KEY, 
            vessel_name TEXT,
            ship_type INTEGER,
            dim_bow INTEGER,
            dim_stern INTEGER,
            dim_port INTEGER,
            dim_star INTEGER,
            imo INTEGER
        ) ''')
    cur.executemany(f''' INSERT INTO static_{month}_aggregate VALUES (?,?,?,?,?,?,?,?) ''', agg_rows)
    conn.commit()
    return





### old, not used

    cur.execute(f"""
    WITH static AS (
        SELECT m5.mmsi, m5.vessel_name, m5.ship_type, m5.dim_bow, m5.dim_stern, 
            m5.dim_port, m5.dim_star, m5.imo, 5 as msgtype
          FROM ais_{month}_msg_5 AS m5
         UNION ALL
        SELECT m24.mmsi, m24.vessel_name, m24.ship_type, m24.dim_bow, m24.dim_stern, 
               m24.dim_port, m24.dim_star, NULL as imo, 24 as msgtype
          FROM ais_{month}_msg_24 AS m24
          ORDER BY 1
    ) 
    --SELECT * FROM static 
    SELECT *, COUNT(*) OVER (PARTITION BY mmsi  ) FROM static 
    """)
    res = np.array(cur.fetchall(), dtype=object)
    res
    res[0:100]

    cur.execute(f''' select count(*) from ais_{month}_msg_5 ''')
    cur.execute(f''' select count(*) from ais_{month}_msg_24 ''')

    cur.execute(f'''
    CREATE TABLE IF NOT EXISTS view_{month}_static AS SELECT * FROM (
        SELECT m5.mmsi, m5.vessel_name, m5.ship_type, m5.dim_bow, m5.dim_stern, m5.dim_port, m5.dim_star, m5.imo, m5.time
        --, COUNT(*) as n 
          FROM ais_{month}_msg_5 AS m5
          GROUP BY m5.mmsi, m5.ship_type, m5.vessel_name
          --HAVING n > 1
        UNION
        SELECT m24.mmsi, m24.vessel_name, m24.ship_type, m24.dim_bow, m24.dim_stern, m24.dim_port, m24.dim_star, 
        COUNT(*) as n
          FROM ais_{month}_msg_24 AS m24
          GROUP BY m24.mmsi, m24.ship_type, m24.vessel_name
          --HAVING n > 1
        ORDER BY 1 , 8 , 2 , 3 
    ) 
    GROUP BY mmsi
    HAVING MAX(n) > 1
    ''')
    res = cur.fetchall()

#fpath = '/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-06-01.csv'
#fpath = '/run/media/matt/My Passport/raw_test/exactEarth_historical_data_2021-04-01.nm4'
#dbpath = '/run/media/matt/My Passport/ais_august.db'
#dbpath = 'output/test_decode.db'
#dbpath = '/run/media/matt/My Passport/test_decode.db'
#dbpath = '/run/media/matt/My Passport/june2018_vacuum.db'
dbpath = '/run/media/matt/My Passport/june2018_test3.db'

#os.remove(dbpath)
decode_raw_pyais(fpath, dbpath)




aisdb = dbconn(dbpath=dbpath)
conn, cur = aisdb.conn, aisdb.cur

cur.execute('select * from rtree_201806_msg_1_2_3 LIMIT 10')
cur.execute('VACUUM INTO "/run/media/matt/My Passport/june2018_vacuum.db"')
cur.execute('VACUUM INTO "/home/matt/june2018_vacuum_test3.db"')
cur.execute('select * from ais_201806_msg_1_2_3 LIMIT 10')
cur.execute('SELECT name FROM sqlite_master WHERE type IN ("table", "view") AND name NOT LIKE "sqlite_%" ORDER BY 1')
res = np.array(cur.fetchall())
res

from shapely.geometry import Polygon, LineString, MultiPoint
from gis import *
from track_viz import *
from track_gen import *



import os

from database import *

testdbs = os.path.join(os.path.dirname(dbpath), 'testdb') + os.path.sep 

"""
month = '201806'
aisdb = dbconn(dbpath=dbpath)
conn, cur = aisdb.conn, aisdb.cur
cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_shiptype ON 'ais_{month}_msg_5' (ship_type) ''')
cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_vesselname ON 'ais_{month}_msg_5' (vessel_name) ''')
conn.close()
"""



if not os.path.isdir(testdbs): 
    os.mkdir(testdbs)


def test_sort_1w():
    db = testdbs + 'june2018-06-01_test.db'
    #os.remove(db)
    #dirpath, dirnames, filenames = np.array(list(os.walk('/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018'))[0], dtype=object)
    dirpath, dirnames, filenames = np.array(list(os.walk(rawdata_dir))[0], dtype=object)
    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2018-06' in f])
    filepaths = filepaths[0:7]
    dt = datetime.now()
    decode_msgs(filepaths, db)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_sort_1m():

    #db= testdbs + '201806.db'
    db= testdbs + '201909.db'
    #os.remove(db)
    dirpath, dirnames, filenames = np.array(list(os.walk(rawdata_dir))[0], dtype=object)
    #filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2018-06' in f])
    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2019-09' in f], dtype=object)
    filepaths = np.array([os.path.join(dirpath, f) for f in sorted(filenames) if '2021-01' in f and '.nm4' in f], dtype=object)
    dt = datetime.now()
    decode_msgs(filepaths, db, processes=32)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_aggregate_staticreports():

    from database import *
    dbpath = '/meridian/aisdb/eE_202009_test2.db'
    aisdb = dbconn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    month = '201806'
    month = '202009'
    aggregate_static_msg5_msg24(dbpath, [month])
    conn.commit()

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
if False:


    cur.execute(f''' select count(*) from ais_{month}_msg_5 ''')
    cur.execute(f''' select count(*) from ais_{month}_msg_24 ''')


#fpath = '/run/media/matt/Seagate Backup Plus Drive1/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-06-01.csv'
#fpath = '/run/media/matt/My Passport/raw_test/exactEarth_historical_data_2021-04-01.nm4'
fpath = '/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2019/CCG_AIS_Log_2019-09-16.csv'
#dbpath = '/run/media/matt/My Passport/ais_august.db'
#dbpath = 'output/test_decode.db'
#dbpath = '/run/media/matt/My Passport/test_decode.db'
#dbpath = '/run/media/matt/My Passport/june2018_vacuum.db'
dbpath = '/run/media/matt/My Passport/june2018_test3.db'

#os.remove(dbpath)
decode_raw_pyais(fpath)




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



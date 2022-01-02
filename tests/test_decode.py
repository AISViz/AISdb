import os

from aisdb import *
from database import *
from aisdb.database.decoder import *
from aisdb.proc_util import glob_files

testdbs = os.path.join(os.path.dirname(dbpath), 'testdb') + os.path.sep 


if not os.path.isdir(testdbs): 
    os.mkdir(testdbs)

def test_sort_1d():

    db = testdbs + 'test_24h.db'
    os.remove(db)
    filepaths = glob_files(rawdata_dir, ext='.nm4')
    testset = [f for f in filepaths if getfiledate(f) - getfiledate(filepaths[0]) <= timedelta(days=1)] 
    dt = datetime.now()
    decode_msgs(testset, db, processes=12, delete=False)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_sort_1w():

    db = testdbs + 'test_8days.db'
    os.remove(db)
    filepaths = glob_files(rawdata_dir, ext='.nm4')
    testset = [f for f in filepaths if getfiledate(f) - getfiledate(filepaths[0]) < timedelta(days=8)] 
    dt = datetime.now()
    decode_msgs(testset, db, processes=12, delete=False)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_sort_1m():

    db = testdbs + 'test_32days.db'
    #os.remove(db)
    filepaths = glob_files(rawdata_dir, ext='.nm4')
    testset = [f for f in filepaths if getfiledate(f) - getfiledate(filepaths[0]) < timedelta(days=32)] 
    dt = datetime.now()
    decode_msgs(testset, db, processes=12)
    delta =datetime.now() - dt
    print(f'total parse and insert time: {delta.total_seconds():.2f}s')


def test_aggregate_staticreports():

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

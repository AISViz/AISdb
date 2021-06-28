import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

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
    june = np.array(sorted(filenames))[ ['2018-06' in filename for filename in sorted(filenames)] ]
    fpath = os.path.join(dirpath, june[0])

    decode_raw_pyais(fpath, dbpath=dbpath)


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
    dbpath = 'output/eE_202009_test.db'
    #os.remove(dbpath)
    dirpath, dirnames, filenames = np.array(list(os.walk('/meridian/AIS_archive/meopar/2020/202009'))[0], dtype=object)

    sept = np.array(sorted(filenames))[ ['.nm4' in filename for filename in sorted(filenames)] ]

    for filename in sept:
        decode_raw_pyais(fpath=os.path.join(dirpath, filename), dbpath=dbpath)


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



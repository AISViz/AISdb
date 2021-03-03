from pyais import FileReaderStream
from datetime import datetime, timedelta
import re

from database import *



# convert 6-bit encoded AIS messages to numpy array format


dbpath = '/run/media/matt/Seagate Backup Plus Drive/python/ais.db'
newdb = not os.path.isfile(dbpath)
conn = sqlite3.connect(dbpath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = conn.cursor()
conn.enable_load_extension(True)
cur.execute('SELECT load_extension("mod_spatialite.so")')
if newdb:
    cur.execute('SELECT InitSpatialMetaData(1)')


datestr = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}')


def decode_csv(fpath, conn, mstr=None):
    cur = conn.cursor()

    mstr = fpath.rsplit('_',1)[1].rsplit('-',1)[0].replace('-','')
    assert int(mstr) >= 201101
    filedate = datestr.search(fpath)
    filedate
    

    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_s_{mstr}_msg_1_2_3" ')
    if not cur.fetchall():
        create_table_msg123(cur, mstr)
        create_table_msg5(cur, mstr)
        create_table_msg18(cur, mstr)
        create_table_msg24(cur, mstr)
        create_table_msg27(cur, mstr)
        create_table_msg_other(cur, mstr)

    rows = np.array([msg.decode().content for msg in FileReaderStream(fpath) if msg.decode() is not None], dtype=object)

    rowtypes = [row['type'] for row in rows]
    assert 11 not in rowtypes, 'UTC date responses not being parsed'
    { m : rowtypes.count(m) for m in np.unique(rowtypes) }
    mtest = np.array([r['type'] == 8 or r['type'] == 24 for r in rows])
    mtest = np.array([r['type'] == 21 for r in rows])
    rows[mtest]

    if len(rows) == 0:
        print('no data! skipping...')
        return

    # construct timestamps - assumes chronological ordering
    # entropy loss: up to 1 minute for 1 in ~25k messages, assuming ~150k messages per file
    # can be minimized further by passing most recent base station report from previous file,
    # but would require parsing files in chronological order

    stamp = datetime(1,1,1)
    rowiter = iter(rows)
    r = next(rowiter)
    while stamp.year < 2000:
        while 'year' not in r.keys() or r['year'] < 2000 or not 0 <= r['second'] <= 60: r = next(rowiter) 
        stamp = datetime(r['year'], r['month'], r['day'], r['hour'], r['minute'], r['second'])
    year, month, day, hour, minute, second = stamp.year, stamp.month, stamp.day, stamp.hour, stamp.minute, stamp.second

    stamps = []
    for row in rows:
        if validyear := ('year' in row.keys() and 2000 <= row['year'] < datetime.now().year): 
            year = row['year']
        if validmonth := (validyear and 'month' in row.keys() and 1 <= row['month'] <= 12): 
            month = row['month']
        if validday := (validmonth and 'day' in row.keys() and 1 <= row['day'] <= 31): 
            day = row['day']
        if validhour := (validday and 'hour' in row.keys() and 0 <= row['hour'] <= 23): 
            hour = row['hour']
        if validmin := (validhour and 'minute' in row.keys() and 0 <= row['minute'] <= 59):
            minute = row['minute']
        if 'second' in row.keys() and 0 <= row['second'] <= 59:
            second = row['second']
        stamps.append(datetime(year, month, day, hour, minute, second))
        print(stamps[-1])
    stamps = np.array(stamps, dtype=object).astype(datetime)

    # insert messages 1,2,3
    m123 = np.array([0 < r['type'] < 4 for r in rows])
    tup123 = ((
                r['type'], r['repeat'], int(r['mmsi']), r['status'].value, r['turn'], 
                r['speed'], r['accuracy'], r['lon'], r['lat'], r['course'], r['heading'], 
                r['second'], r['maneuver'].value, r['raim'], r['radio'], 
                datetime(t.year, t.month, t.day, t.hour, r['second'] if r['second'] < 60 else 0),
                f'''POINT (({r['lon']}, {r['lat']}))''', 
            ) for r,t in zip(rows[m123], stamps[m123])
        )

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_1_2_3 '
                    '(message_id, repeat_indicator, mmsi, navigational_status, rot, '
                    'sog, accuracy, longitude, latitude, cog, heading, utc_second, '
                    'maneuver, raim_flag, communication_state, time, ais_geom) '
                    '''VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,GeomFromText(?, 4326))''', tup123)

    '''
    cur.execute('select * from ais_s_201810_msg_1_2_3 limit 100')
    res = cur.fetchall()
    '''

    # insert message 5
    m5 = np.array([r['type'] == 5 for r in rows])
    #np.unique([r.keys() for r in rows[m5]])
    tup5 = ((
        r['type'], r['repeat'], r['mmsi'], r['ais_version'] , r['imo'], r['callsign'], 
        r['shipname'], r['shiptype'], r['to_bow'], r['to_stern'], r['to_port'], 
        r['to_starboard'], r['epfd'].value, r['month'], r['day'], r['hour'], r['minute'], 
        r['draught'], r['destination'], r['dte']) for r in rows[m5])

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_5 '
                    '(message_id, repeat_indicator, mmsi, ais_version, imo, call_sign, '
                    'vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, '
                    'fixing_device, eta_month, eta_day, eta_hour, eta_minute, draught, '
                    'destination, dte) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup5)

    # insert message 18, 19
    m18 = np.array([r['type'] == 18 or r['type'] == 19 for r in rows])
    tup18 = ((
                r['type'], r['repeat'], r['mmsi'], r['speed'] , r['accuracy'], r['lon'], 
                r['lat'], r['course'], r['heading'], r['second'], r['regional'], r['cs'], 
                r['display'], r['dsc'], r['band'], r['msg22'], r['assigned'], r['raim'], 
                r['radio'],
                #) for r in rows[m18]])
                datetime(t.year, t.month, t.day, t.hour, r['second'] if r['second'] < 60 else 0),
                f'''POINT (({r['lon']}, {r['lat']}))''', 
            ) for r,t in zip(rows[m18], stamps[m18])
        )

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_18 '
                    '(message_id, repeat_indicator, mmsi, sog, accuracy, longitude, '
                    'latitude, cog, heading, utc_second, region, communication_flag, '
                    'display, dsc, band, msg22, mode, raim_flag, communication_state, time, ais_geom) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,GeomFromText(?, 4326))', tup18)

    # insert message 24
    m24 = np.array([r['type'] == 24 for r in rows])
    tup24 = ((
        r['type'], r['repeat'], r['mmsi'], (p := r['partno']), r['shiptype'].value if p else None, 
        r['vendorid'] if p else None, r['model'] if p else None, r['serial'] if p else None, 
        r['callsign'] if p else None, r['to_bow'] if p else None, r['to_stern'] if p else None, 
        r['to_port'] if p else None, r['to_starboard'] if p else None, 
        r['mothership_mmsi'] if p else None, 
        ) for r in rows[m24])

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_24 '
                    '(message_id, repeat_indicator, mmsi, sequence_id, ship_type, vendor_id,  '
                    'model, serial, call_sign, dim_bow, dim_stern, dim_port, dim_star, mother_ship_mmsi) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup24)

    conn.commit()



if __name__ == '__main__':

    folder = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/"
    glob  = list(os.walk(folder))
    csvfiles = [glob[0][0] + f for f in glob[0][2] if f[-4:] == '.csv']

    # assuming WGS84 (EPSG 4326)

    # 28k bad timestamps, 5k good timestamps, 98k incomplete
    # should invalid timestamps be fixed and keep lon/lat or discarded?
    fpath = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-01-21.csv"


    t1 = datetime.now()
    #for fpath in sorted(csvfiles)[194:210]: 
    for fpath in sorted(csvfiles)[17:]: 
        print(fpath)
        decode_csv(fpath, conn)
    t2 = datetime.now()
    print(t2 - t1)
    # about 2 hours to load 1 year of data
    # resulting db ~3GB

    cur.execute('Select * from ais_s_201810_msg_1_2_3 limit 1')
    cur.fetchall()


"""
for msg in FileReaderStream(fpath):
    dmsg = msg.decode()
    #content = dmsg.content
    #print(f'msg: {dmsg.msg_type}  {content.keys()}')
    #rows.append(dmsg.content)
    if not msg.is_single:
        print(dmsg.content)
        if not dmsg.content['type'] == 8: break

        #rows.append([content['mmsi'], content['lon'], content['lat'], content['speed'], content['course'], content['heading'], content['second']])
"""

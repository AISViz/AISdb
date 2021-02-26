from pyais import FileReaderStream
from datetime import datetime, timedelta

from database import *


# convert 6-bit encoded AIS messages to numpy array format


dbpath = '/run/media/matt/Seagate Backup Plus Drive/python/ais.db'
newdb = not os.path.isfile(dbpath)
conn = sqlite3.connect(dbpath)
cur = conn.cursor()
conn.enable_load_extension(True)
cur.execute('SELECT load_extension("mod_spatialite.so")')
if newdb:
    cur.execute('SELECT InitSpatialMetaData(1)')



def decode_csv(fpath, mstr=None):
    #if mstr is None: mstr = fpath.rsplit('_',1)[1].rsplit('-',1)[0].replace('-','')
    mstr = fpath.rsplit('_',1)[1].rsplit('-',1)[0].replace('-','')
    assert int(mstr) >= 201101
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_s_{mstr}_msg_1_2_3" ')
    if not cur.fetchall():
        create_table_msg123(cur, mstr)
        create_table_msg5(cur, mstr)
        create_table_msg18(cur, mstr)
        create_table_msg24(cur, mstr)
        create_table_msg27(cur, mstr)
        create_table_msg_other(cur, mstr)


    rows = np.array([msg.decode().content for msg in FileReaderStream(fpath) if msg.decode() is not None], dtype=object)
    #np.unique([row['type'] for row in rows])

    if len(rows) == 0:
        print('no data! skipping...')
        return

    # TODO: get latest timestamp for index based on base station reports (msg4)
    # append year, month, day, hour, minute to msg3 time

    # insert messages 1,2,3
    m123 = np.array([0 < r['type'] < 4 for r in rows])
    m4 = np.array([r['type'] == 4 for r in rows])
    m123idx = np.array(range(len(rows)))[m123]
    m4idx = np.array(range(len(rows)))[m4]
    timestampidx = 
    #np.unique([r.keys() for r in rows[m123]])
    tup123 = tuple([(r['type'], r['repeat'], int(r['mmsi']), r['status'].value, r['turn'], 
        r['speed'], r['accuracy'], r['lon'], r['lat'], r['course'], r['heading'], 
        r['second'], r['maneuver'].value, r['raim'], r['radio']) for r in rows[m123]])

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_1_2_3 '
                    '(message_id, repeat_indicator, mmsi, navigational_status, rot, '
                    'sog, accuracy, longitude, latitude, cog, heading, utc_second, '
                    'maneuver, raim_flag, communication_state) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup123)

    '''
    cur.execute('select * from ais_s_201810_msg_1_2_3 limit 100')
    res = cur.fetchall()
    '''

    # insert message 5
    m5 = np.array([r['type'] == 5 for r in rows])
    #np.unique([r.keys() for r in rows[m5]])
    tup5 = tuple([(
        r['type'], r['repeat'], r['mmsi'], r['ais_version'] , r['imo'], r['callsign'], 
        r['shipname'], r['shiptype'], r['to_bow'], r['to_stern'], r['to_port'], 
        r['to_starboard'], r['epfd'].value, r['month'], r['day'], r['hour'], r['minute'], 
        r['draught'], r['destination'], r['dte']) for r in rows[m5]])

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_5 '
                    '(message_id, repeat_indicator, mmsi, ais_version, imo, call_sign, '
                    'vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, '
                    'fixing_device, eta_month, eta_day, eta_hour, eta_minute, draught, '
                    'destination, dte) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup5)

    cur.execute(f'SELECT Count(*) from ais_s_{mstr}_msg_1_2_3')

    # insert message 18, 19
    m18 = np.array([r['type'] == 18 for r in rows])
    #np.unique([r.keys() for r in rows[m18]])
    tup18 = tuple([(
        r['type'], r['repeat'], r['mmsi'], r['speed'] , r['accuracy'], r['lon'], 
        r['lat'], r['course'], r['heading'], r['second'], r['regional'], r['cs'], 
        r['display'], r['dsc'], r['band'], r['msg22'], r['assigned'], r['raim'], 
        r['radio']) for r in rows[m18]])

    cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_18 '
                    '(message_id, repeat_indicator, mmsi, sog, accuracy, longitude, '
                    'latitude, cog, heading, utc_second, region, communication_flag, '
                    'display, dsc, band, msg22, mode, raim_flag, communication_state) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup18)

    conn.commit()



if __name__ == '__main__':
    folder = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/"
    glob  = list(os.walk(folder))
    csvfiles = [glob[0][0] + f for f in glob[0][2] if f[-4:] == '.csv']

    fpath = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-10-08.csv"


    t1 = datetime.now()
    for fpath in sorted(csvfiles)[194:210]: 
        print(fpath)
        decode_csv(fpath)
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

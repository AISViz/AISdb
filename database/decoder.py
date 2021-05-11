import os
from datetime import datetime, timedelta
import re
import sqlite3

#from pyais import FileReaderStream
from ais import decode
import numpy as np

from database.create_tables import *


#datestr = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}')


def is_valid_date(year, month, day, hour=0, minute=0, second=0):
    day_count_for_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if year%4==0 and (year%100 != 0 or year%400==0): day_count_for_month[2] = 29
    return (1 <= month <= 12 and 1 <= day <= day_count_for_month[month] 
            and 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59)

def dt_2_epoch(dt_arr, t0=datetime(2000,1,1,0,0,0)):
    delta = lambda dt: (dt - t0).total_seconds()
    if isinstance(dt_arr, (list, np.ndarray)): return list(map(int, map(delta, dt_arr)))
    elif isinstance(dt_arr, (datetime)): return int(delta(dt_arr))
    else: raise ValueError('input must be datetime or array of datetimes')

def epoch_2_dt(ep_arr, t0=datetime(2000,1,1,0,0,0), unit='seconds'):
    delta = lambda ep, unit: t0 + timedelta(**{f'{unit}' : ep})
    if isinstance(ep_arr, (list, np.ndarray)): return list(map(partial(delta, unit=unit), ep_arr))
    elif isinstance(ep_arr, (float, int)): return delta(ep_arr, unit=unit)
    else: raise ValueError('input must be integer or array of integers')


def decode_csv(fpath, conn, mstr=None):
    if not 'ITU123' in fpath and not 'ITU5' in fpath and not 'ITU18' in fpath and not 'ITU24' in fpath:
        print(f'skipped {fpath}')
        return
    cur = conn.cursor()

    with open(fpath, 'r') as f:
        header = f.readline().rstrip('\n').lstrip('"').rstrip('"').split('","')
        msgs = np.array([{k:v for k,v in zip(header, msg.rstrip('\n').lstrip('"').rstrip('"').split('","'))} for msg in f.readlines()])

    msgs = msgs[np.array([msg['Date time stamp'] for msg in msgs]) != '']

    payloads = np.array([msg['Iec message'] for msg in msgs])

    #aivdm, count, fragment, seqid, channel, payload, fill = [msg['Iec message'].split('|')[0].split(',') for msg in msgs]
    #list(map(chr, [char if char-48 <= 40 else char-8 for char in map(ord, fill)]))

    stamps = np.array([datetime.strptime(msg['Date time stamp'], r'%Y-%m-%d %H:%M:%S UTC') for msg in msgs], dtype=datetime)
    epochs = np.array(dt_2_epoch(stamps))
    np.array([msg['Date time stamp'] for msg in msgs]) != ''
    mstr = str(f'{stamps[0].year:04d}{stamps[0].month:02d}')

    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_1_2_3" ')
    if not cur.fetchall():
        #create_table_msg123(cur, mstr)
        sqlite_create_table_msg123(cur, mstr)
        create_table_msg5(cur, mstr)
        #create_table_msg18(cur, mstr)
        sqlite_create_table_msg18(cur, mstr)
        create_table_msg24(cur, mstr)
        create_table_msg27(cur, mstr)
        #create_table_msg_other(cur, mstr)


    # insert messages 1,2,3
    if 'ITU123' in fpath:
        rows = np.array([decode(payload.split(',')[5], 0) for payload in payloads])
        m123 = np.array([0 < r['id'] < 4 for r in rows])
        """
        tup123 = ((
                    int(r['mmsi']), r['nav_status'], r['rot'], 
                    r['sog'], r['x'], r['y'], r['cog'], r['true_heading'], 
                    t.second, 
                    #datetime(t.year, t.month, t.day, t.hour, t.minute, t.second),
                    t,
                    #f'''POINT (({r['x']}, {r['y']}))''', 
                ) for r,t in zip(rows[m123], epochs[m123])
            )
        cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_1_2_3 '
                        '(mmsi, navigational_status, rot, '
                        'sog, longitude, latitude, cog, heading, utc_second, '
                        'time) '
                        #'ais_geom '
                        #'''VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,GeomFromText(?, 4326))''', tup123)
                        '''VALUES (?,?,?,?,?,?,?,?,?,?)''', tup123)
        """

        tup123 = ((
            float(r['mmsi']), float(r['mmsi']), t, t, r['x'], r['x'], r['y'], r['y'], 
            r['nav_status'], r['rot'], r['sog'], r['cog'], r['true_heading'], 
            r['special_manoeuvre'], r['timestamp']
            )   for r,t in zip(rows[m123], epochs[m123].astype(float))
        )
        cur.executemany(f'INSERT OR IGNORE INTO rtree_{mstr}_msg_1_2_3 '
                        '(mmsi0, mmsi1, t0, t1, x0, x1, y0, y1, '
                        'navigational_status, rot, sog, cog, '
                        'heading, maneuver, utc_second) '
                        '''VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', tup123)

    # insert message 5
    elif 'ITU5' in fpath:
        #m5 = np.array([r['id'] == 5 for r in rows])
        m5 = np.array([True for _ in msgs])
        tup5 = ((
                    r['Msg type'], None, r['MMSI'], None , r['IMO number'], r['Callsign'], 
                    r['Name'].rstrip(), r['Type of ship'], r['DimensionA'], r['DimensionB'], r['DimensionC'], 
                    r['DimensionD'], r['Electronic fixing device'], int(r['Eta'][0:2]), int(r['Eta'][2:4]), 
                    int(r['Eta'][4:6]), int(r['Eta'][6:8]), r['Max draught'], r['Destination'], 1) 
                for r,t in zip(msgs[m5], stamps[m5]))

        cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_5 '
                        '(message_id, repeat_indicator, mmsi, ais_version, imo, call_sign, '
                        'vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, '
                        'fixing_device, eta_month, eta_day, eta_hour, eta_minute, draught, '
                        'destination, dte) '
                        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup5)

    # insert message 18, 19
    elif 'ITU18' in fpath:
        rows = np.array([decode(payload.split(',')[5], 0) for payload in payloads])
        m18 = np.array([r['id'] == 18 for r in rows])
        """ 
        tup18 = ((
                    r['mmsi'], r['sog'] , r['x'], 
                    r['y'], r['cog'], r['true_heading'], r['timestamp'], 
                    #datetime(t.year, t.month, t.day, t.hour, t.minute, t.second),
                    t, 
                    #f'''POINT (({r['x']}, {r['y']}))''', 
                ) for r,t in zip(rows[m18], stamps[m18])
            )

        cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_18 '
                        '(mmsi, sog, longitude, '
                        'latitude, cog, heading, utc_second, '
                        'time) '
                        'VALUES (?,?,?,?,?,?,?,?)', tup18)
        """
        tup18 = ((
            float(r['mmsi']), float(r['mmsi']), t, t, r['x'], r['x'], r['y'], r['y'], 
            r['nav_status'] if 'nav_status' in r.keys() else None, 
            r['sog'], r['cog'], r['true_heading'], r['timestamp'],
            )   for r,t in zip(rows[m18], epochs[m18].astype(float))
        )
        cur.executemany(f'INSERT OR IGNORE INTO rtree_{mstr}_msg_18'
                        '(mmsi0, mmsi1, t0, t1, x0, x1, y0, y1, '
                        'navigational_status, sog, cog, '
                        'heading, utc_second) '
                        '''VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', tup18)

    # insert message 24
    elif 'ITU24' in fpath:
        #rows = np.array([decode(payload.split(',')[5], 0) for payload in payloads])
        #rows = np.array([decode(payload.split(',')[5], int(payload.split(',')[6][0])) for payload in payloads])
        m24 = np.array([int(r['Msg type']) == 24 for r in msgs])
        tup24 = ((
            int(r['Msg type']), None, int(r['MMSI']), None, int(r['Type of ship'][1:3]) if not r['Type of ship'] == '' and r['Type of ship'][0] == '[' else None, 
            None, None, None, 
            r['Callsign'], r['DimensionA'] , r['DimensionB'], r['DimensionC'], r['DimensionD'], 
            None, 
            ) for r in msgs[m24])

        cur.executemany(f'INSERT OR IGNORE INTO ais_s_{mstr}_msg_24 '
                        '(message_id, repeat_indicator, mmsi, sequence_id, ship_type, vendor_id,  '
                        'model, serial, call_sign, dim_bow, dim_stern, dim_port, dim_star, mother_ship_mmsi) '
                        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup24)

    conn.commit()
    #m19 = np.array([r['id'] == 19 for r in rows])
    #if sum(m19) > 0: print(f'skipped message 19: {sum(m19)}')
    #m27 = np.array([r['id'] == 27 for r in rows])
    #if sum(m27) > 0: print(f'skipped message 27: {sum(m27)}')
    return



if __name__ == '__main__':

    folder = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/"
    glob  = list(os.walk(folder))
    csvfiles = [os.path.join(glob[0][0],f) for f in glob[0][2] if f[-4:] == '.csv']
    csvfiles2 = [os.path.join(glob[1][0],f) for f in glob[1][2] if f[-4:] == '.csv']

    # assuming WGS84 (EPSG 4326)

    # 28k bad timestamps, 5k good timestamps, 98k incomplete
    # should invalid timestamps be fixed and keep lon/lat or discarded?
    fpath = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-01-21.csv"
    conn = dbconn('/run/media/matt/Seagate Backup Plus Drive/python/ais.db').conn

    t1 = datetime.now()
    for fpath in sorted(csvfiles): 
        print(fpath)
        decode_csv(fpath, conn)
        regexdate = datestr.search(fpath)
        mstr = ''.join(fpath[regexdate.start():regexdate.end()].split('-')[:-1])
        _ = cur.execute(f'Select count(*) from ais_s_{mstr}_msg_1_2_3')
        print(f'{mstr} msg123: {cur.fetchall()[0][0]}')
        _ = cur.execute(f'Select count(*) from ais_s_{mstr}_msg_5')
        print(f'{mstr} msg5: {cur.fetchall()[0][0]}')
    t2 = datetime.now()
    print(t2 - t1)
    # about 2.5 hours to load 1 year of data
    # resulting db ~3.8GB



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

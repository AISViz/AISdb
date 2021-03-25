import os
from datetime import datetime, timedelta
import re
import sqlite3

from pyais import FileReaderStream
import numpy as np

from database.create_tables import *


datestr = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}')


def is_valid_date(year, month, day, hour=0, minute=0, second=0):
    day_count_for_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if year%4==0 and (year%100 != 0 or year%400==0): day_count_for_month[2] = 29
    return (1 <= month <= 12 and 1 <= day <= day_count_for_month[month] 
            and 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59)


def decode_csv(fpath, conn, mstr=None):
    cur = conn.cursor()

    mstr = fpath.rsplit('_',1)[1].rsplit('-',1)[0].replace('-','')
    assert int(mstr) >= 201101
    regexdate = datestr.search(fpath)
    filedate = datetime(*map(int, [(dstr := fpath[regexdate.start():regexdate.end()].replace('-', ''))[0:4], dstr[4:6], dstr[6:8]]))
    maxdelta = timedelta(hours=36)
    

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
    '''
    2018-01-28 only 1000 msg123 ???
    fpath = sorted(csvfiles)[24]
    fpath
    { m : rowtypes.count(m) for m in np.unique(rowtypes) }
    m4 = np.array([r['type'] == 4 for r in rows])
    mtest = np.array([r['type'] == 8 or r['type'] == 24 for r in rows])
    rows[mtest]
    '''

    if len(rows) == 0:
        print('no data! skipping...')
        return

    stamp = datetime(1,1,1)
    rowiter = iter(rows)
    r = next(rowiter)
    while stamp.year < 2000:
        while 'year' not in r.keys() or r['year'] < 2000 or not 0 <= r['second'] <= 60: r = next(rowiter) 
        stamp = datetime(r['year'], r['month'], r['day'], r['hour'], r['minute'], r['second'])
    year, month, day, hour, minute, second = stamp.year, stamp.month, stamp.day, stamp.hour, stamp.minute, stamp.second

    stamps = []
    for row in rows:
        if 1 <= row['type'] <= 3 and row['second'] < 60:
            second = row['second']
        elif (row['type'] == 4 
                and is_valid_date(*(datecols := (row['year'], row['month'], row['day'], row['hour'], row['minute'], row['second'])))
                and filedate - datetime(*datecols) < maxdelta):
            year, month, day = row['year'], row['month'], row['day']
            hour, minute, second = row['hour'], row['minute'], row['second']
        elif (row['type'] == 5 
                and is_valid_date(*(datecols := (year, row['month'], row['day'], row['hour'], row['minute']))) 
                and filedate - datetime(*datecols) < maxdelta):
            month, day, hour, minute = row['month'], row['day'], row['hour'], row['minute']
        elif (row['type'] == 11 
                and is_valid_date(*(datecols := (row['year'], row['month'], row['day'], row['hour'], row['minute'], row['second'])))
                and filedate - datetime(*datecols) < maxdelta):
            year, month, day = row['year'], row['month'], row['day']
            hour, minute, second = row['hour'], row['minute'], row['second']
        elif (row['type'] == 18 
                and row['second'] < 60):
            second = row['second']
        elif row['type'] == 24: 
            pass
        stamps.append(datetime(year, month, day, hour, minute, second))

    stamps = np.array(stamps, dtype=object).astype(datetime)

    # insert messages 1,2,3
    m123 = np.array([0 < r['type'] < 4 and not (r['lat'] == r['lon'] == 0 ) for r in rows])
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

    # insert message 5
    m5 = np.array([r['type'] == 5 for r in rows])
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
    m18 = np.array([r['type'] == 18 and not (r['lat'] == r['lon'] == 0 ) for r in rows])
    tup18 = ((
                r['type'], r['repeat'], r['mmsi'], r['speed'] , r['accuracy'], r['lon'], 
                r['lat'], r['course'], r['heading'], r['second'], r['regional'], r['cs'] if 'cs' in r.keys() else False, 
                r['display'], r['dsc'], r['band'], r['msg22'], r['assigned'], r['raim'], 
                r['radio'],
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
    m19 = np.array([r['type'] == 19 for r in rows])
    if sum(m19) > 0: print(f'skipped message 19: {sum(m19)}')



if __name__ == '__main__':

    folder = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/"
    glob  = list(os.walk(folder))
    csvfiles = [glob[0][0] + f for f in glob[0][2] if f[-4:] == '.csv']

    # assuming WGS84 (EPSG 4326)

    # 28k bad timestamps, 5k good timestamps, 98k incomplete
    # should invalid timestamps be fixed and keep lon/lat or discarded?
    fpath = "/run/media/matt/Seagate Backup Plus Drive/CCG_Terrestrial_AIS_Network/Raw_data/2018/CCG_AIS_Log_2018-01-21.csv"


    t1 = datetime.now()
    for fpath in sorted(csvfiles)[28:]: 
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

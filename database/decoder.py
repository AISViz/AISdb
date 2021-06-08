import os
from datetime import datetime, timedelta
import re
from packaging import version
import logging

import numpy as np
import pyais
from pyais import FileReaderStream
assert version.parse(pyais.__version__) >= version.parse('1.6.1')

from database.create_tables import *



datestr = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}')


from multiprocessing import Pool, Lock
#global dblock = Lock()


def binarysearch(arr, search):
    ''' fast indexing of ordered arrays. used for finding nearest base station report at given index '''
    low, high = 0, len(arr)-1
    while (low <= high):
        mid = int((low + high) / 2)
        if arr[mid] == search or mid == 0 or mid == len(arr)-1:
            return mid
        elif (arr[mid] >= search):
            high = mid -1 
        else:
            low = mid +1
    return mid


def is_valid_date(year, month, day, hour=0, minute=0, second=0, **_):
    ''' check if a given date is a real date '''
    day_count_for_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if year%4==0 and (year%100 != 0 or year%400==0): 
        day_count_for_month[2] = 29
    return ( 2000 <= year   <= datetime.now().year 
            and 1 <= month  <= 12 
            and 1 <= day    <= day_count_for_month[month] 
            and 0 <= hour   <= 23 
            and 0 <= minute <= 59 
            and 0 <= second <= 59)


def dt_2_epoch(dt_arr, t0=datetime(2000,1,1,0,0,0)):
    ''' convert datetime.datetime to epoch seconds '''
    delta = lambda dt: (dt - t0).total_seconds()
    if isinstance(dt_arr, (list, np.ndarray)): return np.array(list(map(int, map(delta, dt_arr))))
    elif isinstance(dt_arr, (datetime)): return int(delta(dt_arr))
    else: raise ValueError('input must be datetime or array of datetimes')


def epoch_2_dt(ep_arr, t0=datetime(2000,1,1,0,0,0), unit='seconds'):
    ''' convert epoch seconds to datetime.datetime '''
    delta = lambda ep, unit: t0 + timedelta(**{f'{unit}' : ep})
    if isinstance(ep_arr, (list, np.ndarray)): return np.array(list(map(partial(delta, unit=unit), ep_arr)))
    elif isinstance(ep_arr, (float, int)): return delta(ep_arr, unit=unit)
    else: raise ValueError('input must be integer or array of integers')


def insert_msg123(cur, mstr, msg123):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_1_2_3" ')
    if not cur.fetchall(): 
        sqlite_create_table_msg123(cur, mstr)
    
    rows, stamps = msg123.T
    epochs = dt_2_epoch(stamps)
    tup123 = ((
        float(r['mmsi']), float(r['mmsi']), e, e, r['lon'], r['lon'], r['lat'], r['lat'], 
        r['status'].value, r['turn'], r['speed'], r['course'], r['heading'], 
        r['maneuver'], r['second']
        )   for r,e in zip(rows, epochs)
    )
    cur.executemany(f'INSERT OR IGNORE INTO rtree_{mstr}_msg_1_2_3 '
                    '(mmsi0, mmsi1, t0, t1, x0, x1, y0, y1, '
                    'navigational_status, rot, sog, cog, '
                    'heading, maneuver, utc_second) '
                    '''VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', tup123)
    return


def insert_msg5(cur, mstr, msg5):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_{mstr}_msg_5" ')
    if not cur.fetchall(): 
        create_table_msg5(cur, mstr)

    rows, stamps = msg5.T
    epochs = dt_2_epoch(stamps)
    tup5 = ((
                r['type'], r['repeat'], int(r['mmsi']), r['ais_version'], r['imo'], r['callsign'], 
                r['shipname'].rstrip(), r['shiptype'], r['to_bow'], r['to_stern'], r['to_port'], 
                r['to_starboard'], r['epfd'], r['month'], r['day'], 
                r['hour'], r['minute'], r['draught'], r['destination'], r['dte'], e
            ) for r,e in zip(rows, epochs)
        )
    cur.executemany(f'INSERT OR IGNORE INTO ais_{mstr}_msg_5 '
                    '(message_id, repeat_indicator, mmsi, ais_version, imo, call_sign, '
                    'vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, '
                    'fixing_device, eta_month, eta_day, eta_hour, eta_minute, draught, '
                    'destination, dte, time) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup5)
    return


def insert_msg18(cur, mstr, msg18):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_18" ')
    if not cur.fetchall(): 
        sqlite_create_table_msg18(cur, mstr)

    rows, stamps = msg18.T
    epochs = dt_2_epoch(stamps)
    tup18 = ((
        float(r['mmsi']), float(r['mmsi']), e, e, r['lon'], r['lon'], r['lat'], r['lat'], 
        r['radio'], #if 'nav_status' in r.keys() else None,
        r['speed'], r['course'], r['heading'], r['second'],
        )   for r,e in zip(rows, epochs)
    )
    cur.executemany(f'INSERT OR IGNORE INTO rtree_{mstr}_msg_18'
                    '(mmsi0, mmsi1, t0, t1, x0, x1, y0, y1, '
                    'navigational_status, sog, cog, '
                    'heading, utc_second) '
                    '''VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', tup18)
    return


def insert_msg24(cur, mstr, msg24):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_{mstr}_msg_24" ')
    if not cur.fetchall(): 
        create_table_msg24(cur, mstr)

    rows, stamps = msg24.T
    epochs = dt_2_epoch(stamps)
    tup24 = ((
            r['type'], r['repeat'], int(r['mmsi']), r['partno'], 
            r['shipname']           if r['partno'] == 0 else None,
            r['shiptype']           if r['partno'] == 1 else None,
            r['vendorid']           if r['partno'] == 1 else None,
            r['model']              if r['partno'] == 1 else None,
            r['serial']             if r['partno'] == 1 else None,
            r['callsign']           if r['partno'] == 1 else None, 
            r['to_bow']             if r['partno'] == 1 else None, 
            r['to_stern']           if r['partno'] == 1 else None, 
            r['to_port']            if r['partno'] == 1 else None, 
            r['to_starboard']       if r['partno'] == 1 else None, 
            r['mothership_mmsi']    if r['partno'] == 1 else None, 
            e,
        ) for r,e in zip(rows, epochs)
    )
    cur.executemany(f'INSERT OR IGNORE INTO ais_{mstr}_msg_24 '
                    '(message_id, repeat_indicator, mmsi, sequence_id, vessel_name, ship_type, vendor_id,  '
                    'model, serial, call_sign, dim_bow, dim_stern, dim_port, dim_star, mother_ship_mmsi, time) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup24)
    return


def decode_chunk(conn, msgs, regexdate, fpathdate, mstr):
    # convert 2-digit year values to 4-digit
    for msg in msgs:
        if 'year' in msg.keys() and msg['year'] + 2000 == fpathdate.year: 
            msg['year'] += 2000  

    # get indexes of base station reports
    msg4idx = np.array(range(len(msgs)))[np.array(['type' in msg.keys() and msg['type'] == 4 for msg in msgs])]
    msg11idx = np.array(range(len(msgs)))[np.array(['type' in msg.keys() and msg['type'] == 11 for msg in msgs])]
    base_reports = lambda msgs, msg4idx: np.array([datetime(**{key: msg[key] for key in ('year', 'month', 'day', 'hour', 'minute', 'second')}) for msg in msgs[msg4idx]])

    if len(msg11idx) > 0: 
        logging.debug(f'warning: skipped {len(msg11idx)} UTC date responses (msg 11)')

    # validate base station reports
    validated = np.array([is_valid_date(**msg) for msg in msgs[msg4idx]]) 
    msg4idx = msg4idx[validated]

    # check if base station report is within 36h of filepath timestamp
    reports1 = base_reports(msgs, msg4idx) 
    in_filetime = np.array([(report - fpathdate) < timedelta(hours=36) for report in reports1])
    msg4idx = msg4idx[in_filetime]

    # check that the timestamp is greater than the preceding message and less than the following message
    reports2 = base_reports(msgs, msg4idx)
    is_sequential = [0]
    for i in range(1, len(reports2)-1):
        if reports2[i] >= reports2[i-1] and reports2[i] <= reports2[i+1]:
            is_sequential.append(i)
    if reports2[-2] <= reports2[-1]: 
        is_sequential.append(len(msg4idx)-1)
    msg4idx = msg4idx[is_sequential]


    # filter messages according to type, get time of nearest base station report, 
    batch = {f'msg{i}' : np.ndarray(shape=(0,2)) for i in (1, 2, 3, 5, 11, 18, 19, 24, 27)}
    for msg, idx in zip(msgs, np.array(range(len(msgs)))):
        # filter irrelevant messages
        nearest = msgs[msg4idx[binarysearch(msg4idx, idx)]]
        if not 'type' in msg.keys() or msg['type'] not in (1, 2, 3, 5, 11, 18, 19, 24, 27): 
            continue
        elif msg['type'] in (5, 24,): 
            basetime = datetime(nearest['year'], nearest['month'], nearest['day'], nearest['hour'], nearest['minute'], nearest['second'])
        elif ('second' in msg.keys() and msg['second'] >= 60 
             or ('lon' in msg.keys() and not -180 <= msg['lon'] <= 180)
             or ('lat' in msg.keys() and not -90 <= msg['lat'] <= 90)
                ):
            logging.debug(f'discarded msg: {msg}')
            continue
        else:
            basetime = datetime(nearest['year'], nearest['month'], nearest['day'], nearest['hour'], nearest['minute'], msg['second'])
        batch[f'msg{msg["type"]}'] = np.vstack((batch[f'msg{msg["type"]}'], [msg, basetime]))

    #batch['msg5'].shape
    cur = conn.cursor()
    msg123 = np.vstack((batch['msg1'], batch['msg2'], batch['msg3']))
    insert_msg123(cur, mstr, msg123)
    insert_msg5(cur, mstr, batch['msg5'])
    insert_msg18(cur, mstr, batch['msg18'])
    insert_msg24(cur, mstr, batch['msg24'])
    conn.commit()


def decode_raw_pyais(fpath, conn, tmpdir='output'):
    logging.info(fpath)

    regexdate = datestr.search(fpath)
    fpathdate = datetime(*list(map(int, fpath[regexdate.start():regexdate.end()].split('-'))))
    mstr      = ''.join(fpath[regexdate.start():regexdate.end()].split('-')[:-1])

    # load msgs from file
    '''
    msgs = []
    for rawmsg in FileReaderStream(fpath):
        msg = rawmsg.decode().content
        msgs.append(msg)
    msgs = np.array(msgs)
    '''
    assert os.path.isdir(tmpdir)

    # copy raw data to tmpfile with extra data removed
    tmpfile = f'{os.path.abspath(tmpdir)}{os.path.sep}{os.getpid()}.nm4' 
    with open(fpath, 'rb') as f, open(tmpfile, 'wb') as o:
        splitmsg = lambda rawmsg: rawmsg.split(b'\\')[-1].rstrip(b'\r\n') + b'\n'
        rawmsgs = np.array(f.readlines())
        o.writelines(map(splitmsg, rawmsgs))

    # stream NMEA messages from tmpfile and decode them in chunks of 100k per transaction
    n = 0
    N = len(rawmsgs) 
    msgs = []
    for msg in FileReaderStream(tmpfile):
        msgs.append(msg.decode().content)
        if len(msgs) > 100000:
            try:
                decode_chunk(conn, np.array(msgs), regexdate, fpathdate, mstr)
            except Exception as e:
                errlog = f'{os.path.abspath(tmpdir)}{os.path.sep}error.log'
                print(f'error, dumping input to {errlog}\n\n{e}\n')
                with open(errlog, 'w') as f:
                    f.writelines([str(msg) for msg in msgs])
                if input('continue? [Y/n]')[0].lower() == 'n': break
            n += len(msgs)
            print(f'\r{n / N * 100:.2f}%', end='')
            msgs = []
    print()

    os.remove(tmpfile)

    '''
    # decode messages or print an error
    failed = 0
    msgs = np.array([])
    #msgs = []
    curtime = fpathdate
    #rawmsg = rawmsgs[5]
    for rawmsg in rawmsgs:
        msg = rawmsg.split(b'\\')[-1].rstrip(b'\r\n')
        try:
            msgdecode = pyais.decode_msg(msg)
        except Exception as e:
            #print(f'{e}')
            #logging.debug(f'{e}')
            failed += 1
            continue
        msgs = np.append(msgs, [msgdecode])
        #msgs.append(msgdecode)
        if len(msgs) > 100000:
            msgs = np.array([])
    #msgs = np.array(msgs)
    logging.info(f'could not decode {failed} messages')
    '''



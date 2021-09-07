import os
import re
from datetime import datetime, timedelta
from packaging import version
import logging
from multiprocessing import Pool#, Lock
from functools import partial
import json
import pickle


import numpy as np
import pyais
from pyais import FileReaderStream
assert version.parse(pyais.__version__) >= version.parse('1.6.1')

from database.create_tables import *
from database import dbconn
from index import index


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
    ''' convert datetime.datetime to epoch minutes '''
    delta = lambda dt: (dt - t0).total_seconds() // 60
    if isinstance(dt_arr, (list, np.ndarray)): return np.array(list(map(int, map(delta, dt_arr))))
    elif isinstance(dt_arr, (datetime)): return int(delta(dt_arr))
    else: raise ValueError('input must be datetime or array of datetimes')


def epoch_2_dt(ep_arr, t0=datetime(2000,1,1,0,0,0), unit='minutes'):
    ''' convert epoch minutes to datetime.datetime '''
    delta = lambda ep, unit: t0 + timedelta(**{f'{unit}' : ep})
    if isinstance(ep_arr, (list, np.ndarray)): return np.array(list(map(partial(delta, unit=unit), ep_arr)))
    elif isinstance(ep_arr, (float, int)): return delta(ep_arr, unit=unit)
    else: raise ValueError('input must be integer or array of integers')


def insert_msg123(cur, mstr, rows):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_1_2_3" ')
    if not cur.fetchall(): 
        sqlite_create_table_msg123(cur, mstr)
    
    tup123 = ((
        float(r['mmsi']), r['epoch'], r['type'], r['lon'], r['lat'], 
        int(r['status']), r['turn'], r['speed'], r['course'], r['heading'], 
        r['maneuver'], r['second'],
        )   for r in rows
    )
    coveridx = ((r['mmsi'], r['epoch']) for r in rows)
    cur.executemany(f'''
                    INSERT OR IGNORE INTO ais_{mstr}_msg_1_2_3 
                    (mmsi, time, msgtype, longitude, latitude, 
                    navigational_status, rot, sog, cog, 
                    heading, maneuver, utc_second) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?) 
                    --ON CONFLICT (mmsi, time) 
                    --DO NOTHING
                    --WHERE NOT EXISTS ( 
                    --SELECT * FROM rtree_{mstr}_msg_1_2_3 AS rtree
                    --    WHERE rtree.mmsi0 = CAST(mmsi AS FLOAT)
                    --    AND rtree.t0 = CAST(time AS FLOAT)
                    --) 
                    '''
                    , tup123)
    return


def insert_msg5(cur, mstr, rows):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_{mstr}_msg_5" ')
    if not cur.fetchall(): 
        create_table_msg5(cur, mstr)

    tup5 = ((
                r['type'], r['repeat'], int(r['mmsi']), r['ais_version'], r['imo'], r['callsign'], 
                r['shipname'].rstrip(), r['shiptype'], r['to_bow'], r['to_stern'], r['to_port'], 
                r['to_starboard'], r['epfd'], r['month'], r['day'], 
                r['hour'], r['minute'], r['draught'], r['destination'], r['dte'], r['epoch']
            ) for r in rows 
        )
    cur.executemany(f'INSERT INTO ais_{mstr}_msg_5 '
                    '(message_id, repeat_indicator, mmsi, ais_version, imo, call_sign, '
                    'vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star, '
                    'fixing_device, eta_month, eta_day, eta_hour, eta_minute, draught, '
                    'destination, dte, time) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup5)
    return


def insert_msg18(cur, mstr, rows):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_18" ')
    if not cur.fetchall(): 
        sqlite_create_table_msg18(cur, mstr)

    tup18 = ((
        int(r['mmsi']), r['epoch'], r['type'], r['lon'], r['lat'], 
        r['radio'] if 'radio' in r.keys() else None,
        r['speed'], r['course'], r['heading'], r['second'],
        ) for r in rows
    )
    cur.executemany(f'''
                    INSERT OR IGNORE INTO ais_{mstr}_msg_18 
                    (mmsi, time, msgtype, longitude, latitude, 
                    navigational_status, sog, cog, 
                    heading, utc_second) 
                    VALUES (?,?,?,?,?,?,?,?,?,?) 
                    --ON CONFLICT (mmsi, time) 
                    --DO NOTHING
                    --WHERE NOT EXISTS ( 
                    --SELECT FROM rtree_{mstr}_msg_18 AS rtree
                    --    WHERE rtree.mmsi0 = CAST(mmsi AS FLOAT)
                    --    AND rtree.t0 = CAST(time AS FLOAT)
                    --) 
                    '''
                    , tup18)
    return


def insert_msg24(cur, mstr, rows):
    cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="ais_{mstr}_msg_24" ')
    if not cur.fetchall(): 
        create_table_msg24(cur, mstr)

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
            r['epoch'],
        ) for r in rows
    )
    cur.executemany(f'INSERT INTO ais_{mstr}_msg_24 '
                    '(message_id, repeat_indicator, mmsi, sequence_id, vessel_name, ship_type, vendor_id,  '
                    'model, serial, call_sign, dim_bow, dim_stern, dim_port, dim_star, mother_ship_mmsi, time) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tup24)
    return


def append_file(picklefile, batch):
    ''' appends batch data to a given picklefile. used by decode_raw_pyais '''
    for key in batch.keys():
        # sort rows by mmsi, type, and epoch timestamp
        rows = np.array(sorted(batch[key], key=lambda r: [r['mmsi'], r['type'], r['epoch']] ), dtype=object)
        # skip empty rows
        if len(rows) == 0: continue
        # skip duplicate epoch-minute timestamps for each mmsi
        keepidx = np.nonzero([x['mmsi']!=y['mmsi'] or x['epoch']!=y['epoch'] for x,y in zip(rows[1:], rows[:-1])])[0]-1
        # write to disk
        with open(f'{picklefile}_{key}', 'ab') as f:
            pickle.dump(rows[keepidx], f)


#def decode_raw_pyais(fpath, tmpdir):
def decode_raw_pyais(fpath):
    ''' parallel process worker function. see decode_msgs() for usage '''

    # if the file was already parsed, skip it

    #with index(storagedir=path, filename=dbfile, bins=False, store=False) as parsed:
    #    if parsed.serialized(kwargs=dict(fpath=fpath)):
    #        return

    splitmsg    = lambda rawmsg: rawmsg.split('\\')
    parsetime   = lambda comment: dt_2_epoch(datetime.fromtimestamp(int(comment.split('c:')[1].split(',')[0].split('*')[0])))

    regexdate   = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}').search(fpath)
    mstr        = ''.join(fpath[regexdate.start():regexdate.end()].split('-')[:-1])
    picklefile  = os.path.join(tmp_dir, ''.join(fpath[regexdate.start():regexdate.end()].split('-')))

    if sum( [os.path.isfile(f'{picklefile}_msg{msgtype}') for msgtype in (1,2,3,5,18,24)] ) >= 6: 
        return

    n           = 0
    skipped     = 0
    failed      = 0
    t0          = datetime.now()
    batch       = {f'msg{i}' : [] for i in (1, 2, 3, 4, 5, 11, 18, 19, 24, 27)}
    #print(f'{fpath.split(os.path.sep)[-1]}\tprocessing message {n}', end='')

    with open(fpath, 'r') as f:
        for rawmsg in f:
            line = splitmsg(rawmsg)

            # check if receiver recorded timestamp. if not, skip the message
            if len(line) > 1 and 'c:' in line[1]:
                stamp, payload = line[1], line[2]
            else:
                skipped +=1
                continue

            # attempt to decode the message
            try:
                msg = pyais.decode_msg(payload)
            except Exception as e:
                failed += 1
                continue

            # discard unused message types, check that data looks OK
            if not 'type' in msg.keys() or msg['type'] not in (1, 2, 3, 4, 5, 11, 18, 19, 24, 27): 
                if not 'type' in msg.keys(): print(msg)
                continue
            elif (  ('lon' in msg.keys() and not -180 <= msg['lon'] <= 180)
                 or ('lat' in msg.keys() and not -90 <= msg['lat'] <= 90)
                    ):
                skipped += 1
                logging.debug(f'discarded msg: {msg}')
                continue
            elif 'radio' in msg.keys() and msg['radio'] > 9223372036854775807:
                skipped += 1
                logging.debug(f'discarded msg: {msg}')
                continue

            # if all ok, log the message and timestamp
            n += 1
            #logging.debug(f'{stamp}  ->  ')
            #logging.debug(f'{parsetime(stamp)}')
            #batch[f'msg{msg["type"]}'].append([msg, parsetime(stamp)])
            msg['epoch'] = parsetime(stamp)
            batch[f'msg{msg["type"]}'].append(msg)

            # every once in a while insert into DB and print a status message
            if n % 100000 == 0: 
                #print(f'\r{fpath.split(os.path.sep)[-1]}\tprocessing message {n}', end='')
                #batch_insert(dbpath=dbpath, batch=batch, mstr=mstr)
                append_file(picklefile, batch)
                batch = {f'msg{i}' : [] for i in (1, 2, 3, 4, 5, 11, 18, 19, 24, 27)}

        #batch_insert(dbpath, batch, mstr)
        append_file(picklefile, batch)

    print(f'{fpath.split(os.path.sep)[-1]}\tprocessed {n} messages in {(datetime.now() - t0).total_seconds():.0f}s.\tskipped: {skipped}\tfailed: {failed}')

    # store a checksum of the filename
    #with index(storagedir=path, filename=dbfile, bins=False, store=False) as parsed:
    #    parsed.insert_hash(kwargs=dict(fpath=fpath))


def decode_msgs(filepaths, dbpath, processes=12):
    ''' decode NMEA binary message format and store in an SQLite database

        messages will be decoded and prepared for insertion in parallel, and 
        parsed results will be serialized and stored in a temporary directory 
        'tmp_parsing' in the same directory as the dbpath file.
        the serialized results will then be ingested into the database in 
        sequence.
        after the messages are loaded into preliminary tables in the database,
        database triggers are used to update the intermediary tables for the 
        dynamic message data. 
        this function will also call aggregate_static_msg5_msg24() to 
        generate an aggregate result table from the static report data

        the intended usage is to store and preprocess messages for an entire
        month at one time, since the intermediary tables require context of the
        entire month when building indexes

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be ingested
                into the database
            dbpath (string)
                location of where the created database should be saved

        returns:
            None
    '''

    # create temporary directory for parsed data
    if not os.path.isdir(tmp_dir): 
        os.mkdir(tmp_dir)

    # decode and serialize
    proc = partial(decode_raw_pyais)
    
    # parallelize decoding step
    with Pool(processes) as p:
        list(p.imap_unordered(proc, filepaths))

    insertfcn = {
            'msg1' : insert_msg123,
            'msg2' : insert_msg123,
            'msg3' : insert_msg123,
            'msg5' : insert_msg5,
            'msg18' : insert_msg18,
            #'msg19' : ,
            'msg24' : insert_msg24,
            #'msg27' : insert_msg123,
        }

    aisdb = dbconn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur

    months_str = []

    # deserialize
    for picklefile in sorted(os.listdir(tmp_dir)):
        msgtype     = picklefile.split('_', 1)[1].rsplit('.', 1)[0]
        regexdate   = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}').search(picklefile)
        mstr        = picklefile[regexdate.start():regexdate.end()][:-2]
        if mstr not in months_str: months_str.append(mstr)

        if msgtype == 'msg11' or msgtype == 'msg27' or msgtype == 'msg19' or msgtype == 'msg4': 
            os.remove(os.path.join(tmp_dir, picklefile))
            continue

        dt = datetime.now()

        with open(os.path.join(tmp_dir, picklefile), 'rb') as f:
            while True:
                try:
                    rows = pickle.load(f)
                except EOFError as e:
                    break
                except Exception as e:
                    raise e
                insertfcn[msgtype](cur, mstr, rows)
        conn.commit()

        delta =datetime.now() - dt
        print(f'insert time {picklefile}:\t{delta.total_seconds():.2f}s')
        os.remove(os.path.join(tmp_dir, picklefile))

    conn.close()
    # aggregate and index static reports: msg5, msg24
    aggregate_static_msg5_msg24(dbpath, months_str)





'''
# old code for decoding with inferred timestamps instead of using receiver report
# may be useful for data that arrives without a receiver timestmap

def decode_chunk(conn, msgs, stamps):
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
    #batch = {f'msg{i}' : np.ndarray(shape=(0,2)) for i in (1, 2, 3, 5, 11, 18, 19, 24, 27)}
    batch = {f'msg{i}' : [] for i in (1, 2, 3, 5, 11, 18, 19, 24, 27)}
    for msg, stamp in zip(msgs, stamps):
        #batch[f'msg{msg["type"]}'] = np.vstack((batch[f'msg{msg["type"]}'], [msg, basetime]))
        batch[f'msg{msg["type"]}'].append([msg, basetime])

'''


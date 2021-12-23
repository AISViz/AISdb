''' parsing NMEA messages to create an SQL database. See function decode_msgs() for usage ''' 

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

from aisdb.common import *
from database.create_tables import createfcns
from database.insert_tables import insertfcns
from gis import dt_2_epoch, epoch_2_dt
from database.dbconn import DBConn
from index import index


datefcn = lambda fpath: re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}').search(fpath)
regexdate_2_dt = lambda reg, fmt='%Y%m%d': datetime.strptime(reg.string[reg.start():reg.end()], fmt)
getfiledate = lambda fpath, fmt='%Y%m%d': regexdate_2_dt(datefcn(fpath), fmt=fmt)


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


def append_file(picklefile, batch):
    ''' appends batch data to a given picklefile. used by decode_raw_pyais '''
    for key in batch.keys():
        # sort rows by mmsi, type, and epoch timestamp
        rows = np.array(sorted(batch[key], key=lambda r: [r['mmsi'], r['type'], r['epoch']] ), dtype=object)
        # skip empty rows
        if len(rows) == 0: continue
        # skip duplicate epoch-minute timestamps for each mmsi
        #skipidx = np.nonzero([x['mmsi']==y['mmsi'] and x['epoch']==y['epoch'] for x,y in zip(rows[1:], rows[:-1])])[0]-1
        skipidx = np.nonzero([
                x['mmsi']==y['mmsi'] 
            and x['type'] == y['type']
            and x['epoch'] == y['epoch'] 
            for x,y in zip(rows[1:], rows[:-1])])[0]

        # write to disk
        with open(f'{picklefile}_{key}', 'ab') as f:
            pickle.dump(rows[~skipidx], f)


#def decode_raw_pyais(fpath, tmpdir):
def decode_raw_pyais(fpath, tmp_dir=tmp_dir):
    ''' parallel process worker function. see decode_msgs() for usage 

        arg:
            fpath: string
                filepath to .nm4 AIS binary data
            tmp_dir: string
                filepath to temporary directory for storing serialized decoded binary

        decodes AIS messages using pyais.
        timestamps are parsed from base station epochs and converted to epoch-minutes.
        Discards messages not in types (1, 2, 3, 4, 5, 11, 18, 19, 24, 27).
        Remaining messages are collected by type, and then serialized. 
        Can be run concurrently 
    '''

    # if the file was already parsed, skip it

    #with index(storagedir=path, filename=dbfile, bins=False, store=False) as parsed:
    #    if parsed.serialized(kwargs=dict(fpath=fpath)):
    #        return

    splitmsg    = lambda rawmsg: rawmsg.split('\\')
    parsetime   = lambda comment: dt_2_epoch(datetime.fromtimestamp(int(comment.split('c:')[1].split(',')[0].split('*')[0])))

    #regexdate   = datefcn(fpath)
    
    #mstr        = ''.join(fpath[regexdate.start():regexdate.end()].split('-')[:-1])
    filedate = getfiledate(fpath)
    mstr = filedate.strftime('%Y%m')
    #picklefile  = os.path.join(tmp_dir, ''.join(fpath[regexdate.start():regexdate.end()].split('-')))
    picklefile = os.path.join(tmp_dir, f'{filedate.strftime("%Y%m%d")}_{fpath.rsplit(".",1)[0].rsplit(os.path.sep, 1)[1]}')

    #if sum( [os.path.isfile(f'{picklefile}_msg{msgtype:02}') for msgtype in (1,2,3,5,18,24)] ) >= 6: 
    #    return

    n           = 0
    skipped     = 0
    failed      = 0
    t0          = datetime.now()
    batch       = {f'msg{i}' : [] for i in (1, 2, 3, 4, 5, 11, 18, 19, 24, 27)}

    with open(fpath, 'r') as f:
        for rawmsg in f:
            line = splitmsg(rawmsg)

            # check if receiver recorded timestamp. if not, skip the message
            if len(line) > 2 and 'c:' in line[1]:
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


def insert_serialized(dbpath, delete=True):

    print('deserializing decoded data and performing DB insert...')

    aisdb = DBConn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    months_str = []

    for serialized in sorted(os.listdir(tmp_dir)):
        msgtype     = serialized.rsplit('_', 1)[1]
        #regexdate   = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}').search(serialized)
        serialdate  = getfiledate(serialized)
        #mstr        = serialized[regexdate.start():regexdate.end()][:-2]
        mstr        = serialdate.strftime('%Y%m')
        if mstr not in months_str: months_str.append(mstr)

        if msgtype == 'msg11' or msgtype == 'msg27' or msgtype == 'msg19' or msgtype == 'msg4': 
            os.remove(os.path.join(tmp_dir, serialized))
            continue

        cur.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="rtree_{mstr}_msg_1_2_3" ')
        if not cur.fetchall(): 
            print(f'creating database tables for month {mstr}...')
            for fcn in createfcns.values(): fcn(cur, mstr)

        dt = datetime.now()

        cur.execute('BEGIN EXCLUSIVE TRANSACTION')
        with open(os.path.join(tmp_dir, serialized), 'rb') as f:
            while True:
                try:
                    rows = pickle.load(f)
                except EOFError as e:
                    break
                except Exception as e:
                    raise e
                insertfcns[msgtype](cur, mstr, rows)
        cur.execute('COMMIT TRANSACTION')
        conn.commit()

        delta =datetime.now() - dt
        print(f'insert time {serialized}:\t{delta.total_seconds():.2f}s')
        if delete:
            os.remove(os.path.join(tmp_dir, serialized))

    conn.close()

    # aggregate and index static reports: msg5, msg24
    aggregate_static_msg5_msg24(dbpath, months_str)


def decode_msgs(filepaths, dbpath, processes=12, delete=True):
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
            processes: int
                number of processes to run in parallel. Set to 0 or False to 
                disable concurrency
            delete: boolean
                if True, decoded data in tmp_dir will be removed

        returns:
            None
    '''

    assert os.listdir(tmp_dir) == [], (
            '''error: tmp directory not empty! '''
            f''' please remove old temporary files in {tmp_dir} before continuing.\n'''
            '''to continue with serialized decoded files as-is without repeating the decoding step, '''
            '''insert them into the database as follows:\n'''
            '''from ais.database.decoder import insert_serialized: \n'''
            '''insert_serialized(filepaths, dbpath)\n''')


    # skip filepaths which were already inserted into the database
    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for i in range(len(filepaths)-1, -1, -1):
            if dbindex.serialized(seed=os.path.abspath(filepaths[i])): 
                skipfile = filepaths.pop(i)
                logging.debug(f'skipping {skipfile}')
            else:
                logging.debug(f'preparing {filepaths[i]}')

    if len(filepaths) == 0: 
        insert_serialized(dbpath, delete=delete)
        return

    # create temporary directory for parsed data
    if not os.path.isdir(tmp_dir): 
        os.mkdir(tmp_dir)

    # decode and serialize
    proc = partial(decode_raw_pyais)
    
    # parallelize decoding step
    print(f'decoding messages... results will be placed temporarily in {tmp_dir} until database insert')
    if processes:
        with Pool(processes) as p:
            list(p.imap_unordered(proc, filepaths))
            p.close()
            p.join()
    else:
        for fpath in filepaths:
            print(fpath)
            proc(fpath)

    insert_serialized(dbpath, delete=delete)

    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for fpath in filepaths:
            dbindex.insert_hash(seed=os.path.abspath(fpath))






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


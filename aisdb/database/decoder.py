''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

import os
import re
from datetime import datetime
import logging
from multiprocessing import Pool  # Lock
from functools import partial
import pickle

import numpy as np
import pyais

from aisdb.common import tmp_dir
from database.create_tables import createfcns, aggregate_static_msgs
from database.insert_tables import insertfcns
from gis import dt_2_epoch
from database.dbconn import DBConn
from index import index


def datefcn(fpath):
    return re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{8}').search(fpath)


def regexdate_2_dt(reg, fmt='%Y%m%d'):
    return datetime.strptime(reg.string[reg.start():reg.end()], fmt)


def getfiledate(fpath, fmt='%Y%m%d'):
    d = datefcn(fpath)
    if d is None:
        print(f'warning: could not parse YYYYmmdd format date from {fpath}!')
        print('warning: defaulting to epoch zero!')
        return datetime(1970, 1, 1)
    fdate = regexdate_2_dt(d, fmt=fmt)
    return fdate


def is_valid_date(year, month, day, hour=0, minute=0, second=0, **_):
    ''' check if a given date is a real date '''
    day_count_for_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
        day_count_for_month[2] = 29
    return (2000 <= year <= datetime.now().year and 1 <= month <= 12
            and 1 <= day <= day_count_for_month[month] and 0 <= hour <= 23
            and 0 <= minute <= 59 and 0 <= second <= 59)


def append_file(picklefile, batch):
    ''' appends batch data to a given picklefile. used by decode_raw_pyais '''
    for key in batch.keys():
        # sort rows by mmsi, type, and epoch timestamp
        rows = np.array(sorted(
            batch[key], key=lambda r: [r['mmsi'], r['type'], r['epoch']]),
                        dtype=object)
        # skip empty rows
        if len(rows) == 0:
            continue
        # skip duplicate epoch-minute timestamps for each mmsi
        #skipidx = np.nonzero([x['mmsi']==y['mmsi'] and x['epoch']==y['epoch'] for x,y in zip(rows[1:], rows[:-1])])[0]-1
        skipidx = np.nonzero([
            x['mmsi'] == y['mmsi'] and x['type'] == y['type']
            and x['epoch'] == y['epoch'] for x, y in zip(rows[1:], rows[:-1])
        ])[0]

        # write to disk
        with open(f'{picklefile}_{key}', 'ab') as f:
            pickle.dump(rows[~skipidx], f)


# def decode_raw_pyais(fpath, tmpdir):
def decode_raw_pyais(fpath, tmp_dir=tmp_dir):
    ''' parallel process worker function. see decode_msgs() for usage

        arg:
            fpath: (string)
                filepath to .nm4 AIS binary data
            tmp_dir: (string)
                filepath to temporary directory for storing serialized decoded
                binary

        decodes AIS messages using pyais.
        timestamps are parsed from base station epochs and converted to
        epoch-minutes. Discards messages not in types
        (1, 2, 3, 4, 5, 11, 18, 19, 24, 27). Remaining messages are collected
        by type, and then serialized.
        Can be run concurrently
    '''

    splitmsg = lambda rawmsg: rawmsg.split('\\')
    parsetime = lambda comment: dt_2_epoch(
        datetime.fromtimestamp(
            int(comment.split('c:')[1].split(',')[0].split('*')[0])))

    filedate = getfiledate(fpath)
    picklefile = os.path.join(
        tmp_dir, f'''{
                filedate.strftime("%Y%m%d")
                }_{
                    fpath.rsplit(".",1)[0].rsplit(os.path.sep, 1)[1]
                    }''')

    n = 0
    skipped = 0
    failed = 0
    t0 = datetime.now()
    batch = {f'msg{i}': [] for i in (1, 2, 3, 4, 5, 11, 18, 19, 24, 27)}

    with open(fpath, 'r') as f:
        for rawmsg in f:
            line = splitmsg(rawmsg)

            # check if receiver recorded timestamp. if not, skip the message
            if len(line) > 2 and 'c:' in line[1]:
                stamp, payload = line[1], line[2]
            else:
                skipped += 1
                continue

            # attempt to decode the message
            try:
                msg = pyais.decode_msg(payload)
            except Exception:
                failed += 1
                continue

            # discard unused message types, check that data looks OK
            if 'type' not in msg.keys() or msg['type'] not in (1, 2, 3, 4, 5,
                                                               11, 18, 19, 24,
                                                               27):
                if 'type' not in msg.keys():
                    print(msg)
                continue
            elif (('lon' in msg.keys() and not -180 <= msg['lon'] <= 180)
                  or ('lat' in msg.keys() and not -90 <= msg['lat'] <= 90)):
                skipped += 1
                logging.debug(f'discarded msg: {msg}')
                continue
            elif 'radio' in msg.keys() and msg['radio'] > 9223372036854775807:
                skipped += 1
                logging.debug(f'discarded msg: {msg}')
                continue

            # if all ok, log the message and timestamp
            n += 1
            msg['epoch'] = parsetime(stamp)
            batch[f'msg{msg["type"]}'].append(msg)

            # every once in a while insert into DB and print a status message
            if n % 100000 == 0:
                append_file(picklefile, batch)
                batch = {
                    f'msg{i}': []
                    for i in (1, 2, 3, 4, 5, 11, 18, 19, 24, 27)
                }

        append_file(picklefile, batch)

    print(f'''{
        fpath.split(os.path.sep)[-1]
        }\tprocessed {n} messages in {
        (datetime.now() - t0).total_seconds():.0f}s.\tskipped: {
        skipped
        }\tfailed: {
        failed}\trate: {
        n / (datetime.now() - t0).total_seconds():.1f}/s''')


def insert_serialized(dbpath, delete=True):

    print('deserializing decoded data and performing DB insert...')

    aisdb = DBConn(dbpath=dbpath)
    conn, cur = aisdb.conn, aisdb.cur
    months_str = []

    for serialized in sorted(os.listdir(tmp_dir)):
        if '_' not in serialized:
            print(f'skipping {serialized}...')
            continue
        msgtype = serialized.rsplit('_', 1)[1]
        serialdate = getfiledate(serialized)
        mstr = serialdate.strftime('%Y%m')
        if mstr not in months_str:
            months_str.append(mstr)

        if (msgtype == 'msg11' or msgtype == 'msg27' or msgtype == 'msg19'
                or msgtype == 'msg4'):
            os.remove(os.path.join(tmp_dir, serialized))
            continue

        cur.execute('SELECT name FROM sqlite_master WHERE type="table" '
                    f'AND name="rtree_{mstr}_msg_1_2_3" ')
        if not cur.fetchall():
            print(f'creating database tables for month {mstr}...')
            for fcn in createfcns.values():
                fcn(cur, mstr)

        dt = datetime.now()

        cur.execute('BEGIN EXCLUSIVE TRANSACTION')
        with open(os.path.join(tmp_dir, serialized), 'rb') as f:
            while True:
                try:
                    rows = pickle.load(f)
                except EOFError:
                    break
                except Exception as e:
                    raise e
                insertfcns[msgtype](cur, mstr, rows)
        cur.execute('COMMIT TRANSACTION')
        conn.commit()

        delta = datetime.now() - dt
        print(f'insert time {serialized}:\t{delta.total_seconds():.2f}s')
        if delete:
            os.remove(os.path.join(tmp_dir, serialized))

    conn.close()

    # aggregate and index static reports: msg5, msg24
    aggregate_static_msgs(dbpath, months_str)


def decode_msgs(filepaths, dbpath, processes=12, delete=True):
    ''' decode NMEA binary message format and store in an SQLite database

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be
                ingested into the database
            dbpath (string)
                location of where the created database should be saved
            processes (int)
                number of processes to run in parallel. Set to 0 or False to
                disable parallelization.
                If Rust is installed, this option is ignored.
            delete (boolean)
                if True, decoded data in tmp_dir will be removed.
                If Rust is installed, this option is ignored

        returns:
            None

        example:

        >>> from aisdb import dbpath, decode_msgs
        >>> filepaths = ['~/ais/rawdata_dir/20220101.nm4',
        ...              '~/ais/rawdata_dir/20220102.nm4']
        >>> decode_msgs(filepaths, dbpath)
    '''
    rustbinary = os.path.join(os.path.dirname(__file__), '..', '..',
                              'aisdb_rust', 'target', 'release', 'aisdb')
    if os.path.isfile(rustbinary):
        files_str = ' --file '.join(["'" + f + "'" for f in filepaths])
        x = (f"{rustbinary} --dbpath '{dbpath}' --file {files_str}")
        os.system(x)
        return

    assert os.listdir(tmp_dir) == [], (
        '''error: tmp directory not empty! '''
        f''' please remove old temporary files in {tmp_dir} before '''
        '''continuing.\n'''
        '''to continue with serialized decoded files as-is without '''
        '''repeating the decoding step, '''
        '''insert them into the database as follows:\n'''
        '''from ais.database.decoder import insert_serialized: \n'''
        '''insert_serialized(filepaths, dbpath)\n''')

    # skip filepaths which were already inserted into the database
    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for i in range(len(filepaths) - 1, -1, -1):
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
    print(
        f'decoding messages... results will be placed temporarily in {tmp_dir} until database insert'
    )
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

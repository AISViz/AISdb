''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

import os
import re
from datetime import datetime
import logging
import subprocess

from aisdb.common import tmp_dir
from aisdb.index import index


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


def decode_msgs(filepaths, dbpath):
    ''' Decode NMEA format AIS messages and store in an SQLite database.
        To speed up decoding, create the database on a different hard drive
        from where the raw data is stored.

        Rust must be installed for this function to work.

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be
                ingested into the database
            dbpath (string)
                location of where the created database should be saved

        returns:
            None

        example:

        >>> from aisdb import dbpath, decode_msgs
        >>> filepaths = ['~/ais/rawdata_dir/20220101.nm4',
                ...              '~/ais/rawdata_dir/20220102.nm4']
        >>> decode_msgs(filepaths, dbpath)
    '''
    assert len(filepaths) > 0
    rustbinary = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', 'aisdb_rust',
                     'target', 'release', 'aisdb'))
    assert os.path.isfile(rustbinary), 'cant find rust executable!'

    # skip filepaths which were already inserted into the database
    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for i in range(len(filepaths) - 1, -1, -1):
            if dbindex.serialized(seed=os.path.abspath(filepaths[i])):
                skipfile = filepaths.pop(i)
                logging.info(f'skipping {skipfile}')
            else:
                logging.debug(f'preparing {filepaths[i]}')

    files_str = []
    for f in filepaths:
        files_str += ['--file', f]
    x = [rustbinary, '--dbpath', dbpath] + files_str
    subprocess.run(x, check=True)

    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for fpath in filepaths:
            dbindex.insert_hash(seed=os.path.abspath(fpath))

    return

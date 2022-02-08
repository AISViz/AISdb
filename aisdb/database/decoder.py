''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

import os
import re
from datetime import datetime
import logging

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


def decode_msgs(filepaths, dbpath, delete=True):
    ''' Decode NMEA format AIS messages and store in an SQLite database.
        To speed up decoding, create the database on a different hard drive
        from where the raw data is stored.

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be
                ingested into the database
            dbpath (string)
                location of where the created database should be saved
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
    assert os.path.isfile(rustbinary)
    if os.path.isfile(rustbinary):
        files_str = ' --file '.join(["'" + f + "'" for f in filepaths])
        x = (f"{rustbinary} --dbpath '{dbpath}' --file {files_str}")
        os.system(x)
        return
    """
    assert os.listdir(tmp_dir) == [], (
        '''error: tmp directory not empty! '''
        f''' please remove old temporary files in {tmp_dir} before '''
        '''continuing.\n'''
        '''to continue with serialized decoded files as-is without '''
        '''repeating the decoding step, '''
        '''insert them into the database as follows:\n'''
        '''from ais.database.decoder import insert_serialized: \n'''
        '''insert_serialized(filepaths, dbpath)\n''')
    """

    # skip filepaths which were already inserted into the database
    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for i in range(len(filepaths) - 1, -1, -1):
            if dbindex.serialized(seed=os.path.abspath(filepaths[i])):
                skipfile = filepaths.pop(i)
                logging.debug(f'skipping {skipfile}')
            else:
                logging.debug(f'preparing {filepaths[i]}')
    """
    if len(filepaths) == 0:
        insert_serialized(dbpath, delete=delete)
        return
    """

    # create temporary directory for parsed data
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)

    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for fpath in filepaths:
            dbindex.insert_hash(seed=os.path.abspath(fpath))

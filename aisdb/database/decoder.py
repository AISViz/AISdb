''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

import os
from hashlib import md5

from aisdb.index import index
from aisdb.database.dbconn import DBConn
import aisdb


def decode_msgs(filepaths, dbpath, source, vacuum=False, skip_checksum=False):
    ''' Decode NMEA format AIS messages and store in an SQLite database.
        To speed up decoding, create the database on a different hard drive
        from where the raw data is stored.
        A checksum of the first kilobyte of every file will be stored to
        prevent loading the same file twice.

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be
                ingested into the database
            dbpath (string)
                database filepath
            source (string)
                data source name or description. will be used as a primary key
                column, so duplicate messages from different sources will not be
                ignored as duplicates upon insert
            vacuum (boolean, str)
                if True, the database will be vacuumed after completion.
                if string, the database will be vacuumed into the filepath
                given.
                Its recommended to supply a filepath string on a seperate
                hard drive from dbpath to increase vacuum speed.
                This will result in a smaller database but takes a long
                time for large datasets

        returns:
            None

        example:

        >>> from aisdb import dbpath, decode_msgs
        >>> filepaths = ['~/ais/rawdata_dir/20220101.nm4',
        ...              '~/ais/rawdata_dir/20220102.nm4']
        >>> decode_msgs(filepaths, dbpath)
    '''
    if len(filepaths) == 0:
        raise ValueError('must supply atleast one filepath.')

    if ':memory:' not in dbpath:
        dbdir, dbname = dbpath.rsplit(os.path.sep, 1)
    else:
        dbdir, dbname = '', ':memory:'

    with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
        for file in filepaths:
            if not skip_checksum:
                with open(os.path.abspath(file), 'rb') as f:
                    signature = md5(f.read(1000)).hexdigest()
                    if file[-4:] == '.csv':  # skip header row (~1.6kb)
                        _ = f.read(600)
                        signature = md5(f.read(1000)).hexdigest()
                if dbindex.serialized(seed=signature):
                    print(f'found matching checksum, skipping {file}')
                    continue
            aisdb.rustdecoder(dbpath=dbpath, files=[file], source=source)
            if not skip_checksum:
                dbindex.insert_hash(seed=signature)

    if vacuum is not False:
        print("finished parsing data\nvacuuming...")
        db = DBConn(dbpath)
        if vacuum is True:
            db.cur.execute("VACUUM")
        elif isinstance(vacuum, str):
            assert not os.path.isfile(vacuum)
            db.cur.execute(f"VACUUM INTO {vacuum}")
        else:
            raise ValueError('vacuum arg must be boolean or filepath string')
        db.conn.commit()
        db.conn.close()

    return

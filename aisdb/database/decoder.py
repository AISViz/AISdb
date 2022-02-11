''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

import os
import subprocess
from hashlib import md5

from aisdb.index import index
from aisdb.database.dbconn import DBConn


def decode_msgs(filepaths, dbpath):
    ''' Decode NMEA format AIS messages and store in an SQLite database.
        To speed up decoding, create the database on a different hard drive
        from where the raw data is stored.
        A checksum of the first kilobyte of every file will be stored to
        prevent loading the same file twice.

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
    dbdir, dbname = dbpath.rsplit(os.path.sep, 1)

    # decode the raw data files, skipping any with matching checksums
    for file in filepaths:
        with open(os.path.abspath(file), 'rb') as f:
            signature = md5(f.read(1000)).hexdigest()

        with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
            if dbindex.serialized(seed=signature):
                print(f'found matching checksum, skipping {file}')
                continue

        x = [rustbinary, '--dbpath', dbpath, '--file', file]
        subprocess.run(x, check=True)

        with index(bins=False, storagedir=dbdir, filename=dbname) as dbindex:
            dbindex.insert_hash(seed=signature)

    print("finished parsing data\nvacuuming...")
    db = DBConn(dbpath)
    db.cur.execute("VACUUM")
    db.conn.commit()
    db.conn.close()

    return

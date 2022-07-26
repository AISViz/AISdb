''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

import os
from hashlib import md5

from aisdb.index import index
from aisdb.database.dbconn import DBConn, get_dbname
from aisdb.aisdb import decoder


def decode_msgs(filepaths,
                dbconn,
                dbpath,
                source,
                vacuum=False,
                skip_checksum=False):
    ''' Decode NMEA format AIS messages and store in an SQLite database.
        To speed up decoding, create the database on a different hard drive
        from where the raw data is stored.
        A checksum of the first kilobyte of every file will be stored to
        prevent loading the same file twice.

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be
                ingested into the database
            db (:class:`aisdb.database.dbconn.DBConn`)
                database connection object
            source (string)
                data source name or description. will be used as a primary key
                column, so duplicate messages from different sources will not be
                ignored as duplicates upon insert
            vacuum (boolean, str)
                if True, the database will be vacuumed after completion.
                if string, the database will be vacuumed into the filepath
                given. Consider vacuuming to second hard disk to speed this up

        returns:
            None

        example:

            >>> from aisdb import dbpath, decode_msgs
        >>> filepaths = ['~/ais/rawdata_dir/20220101.nm4',
                ...              '~/ais/rawdata_dir/20220102.nm4']
        >>> decode_msgs(filepaths, dbpath)
    '''
    if not isinstance(dbconn, DBConn):  # pragma: no cover
        if isinstance(dbconn):
            raise ValueError('Files must be decoded synchronously!')
        raise ValueError('db argument must be a DBConn database connection. '
                         f'got {DBConn}')

    if len(filepaths) == 0:  # pragma: no cover
        raise ValueError('must supply atleast one filepath.')

    dbconn.attach(dbpath)

    hashmap_dbdir, hashmap_dbname = dbpath.rsplit(os.path.sep, 1)

    with index(bins=False, storagedir=hashmap_dbdir,
               filename=hashmap_dbname) as dbindex:
        for file in filepaths:
            # fdate = getfiledate(file)
            if not skip_checksum:
                with open(os.path.abspath(file), 'rb') as f:
                    signature = md5(f.read(1000)).hexdigest()
                    if file[-4:] == '.csv':  # skip header row (~1.6kb)
                        _ = f.read(600)
                        signature = md5(f.read(1000)).hexdigest()
                if dbindex.serialized(seed=signature):
                    print(f'found matching checksum, skipping {file}')
                    continue
            decoder(dbpath=dbpath, files=[file], source=source)
            if not skip_checksum:
                dbindex.insert_hash(seed=signature)

    dbname = get_dbname(dbpath)
    if vacuum is not False:
        print("finished parsing data\nvacuuming...")
        if vacuum is True:
            dbconn.execute(f'VACUUM {dbname}')
        elif isinstance(vacuum, str):
            assert not os.path.isfile(vacuum)
            dbconn.execute(f"VACUUM '{dbname}' INTO '{vacuum}'")
        else:
            raise ValueError('vacuum arg must be boolean or filepath string')
        dbconn.commit()

    return

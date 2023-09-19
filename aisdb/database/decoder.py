''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

from hashlib import md5
from functools import partial
from copy import deepcopy
from datetime import timedelta
import gzip
import os
import pickle
import tempfile
import zipfile

from dateutil.rrule import rrule, MONTHLY
import psycopg

from aisdb.aisdb import decoder
from aisdb.database.dbconn import SQLiteDBConn, PostgresDBConn
from aisdb.proc_util import getfiledate
from aisdb import sqlpath


class FileChecksums():

    def __init__(self, *, dbconn):
        assert isinstance(dbconn, (PostgresDBConn, SQLiteDBConn))
        self.dbconn = dbconn
        self.checksums_table()
        if not os.path.isdir(
                '/tmp') and os.name == 'posix':  # pragma: no cover
            os.mkdir('/tmp')
        self.tmp_dir = tempfile.mkdtemp()
        if not os.path.isdir(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def checksums_table(self):
        ''' instantiates new database connection and creates a checksums
            hashmap table if it doesn't exist yet.

            creates a temporary directory and saves path to ``self.tmp_dir``

            creates SQLite connection attribute ``self.dbconn``, which should
            be closed after use

            e.g.
                self.dbconn.close()
        '''
        cur = self.dbconn.cursor()
        if isinstance(self.dbconn, SQLiteDBConn):
            cur.execute('''
                CREATE TABLE IF NOT EXISTS
                hashmap(
                    hash INTEGER PRIMARY KEY,
                    bytes BLOB
                )
                ''')
        elif isinstance(self.dbconn, PostgresDBConn):
            cur.execute('''
                CREATE TABLE IF NOT EXISTS
                hashmap(
                    hash TEXT PRIMARY KEY,
                    bytes BYTEA
                );''')

        cur.execute('CREATE UNIQUE INDEX '
                    'IF NOT EXISTS '
                    'idx_map on hashmap(hash)')
        self.dbconn.commit()

    def insert_checksum(self, checksum):
        if isinstance(self.dbconn, SQLiteDBConn):
            self.dbconn.execute('INSERT INTO hashmap VALUES (?,?)',
                                [checksum, pickle.dumps(None)])
        elif isinstance(self.dbconn, PostgresDBConn):
            self.dbconn.execute(
                'INSERT INTO hashmap VALUES ($1,$2) ON CONFLICT DO NOTHING',
                [checksum, pickle.dumps(None)])

    def checksum_exists(self, checksum):
        cur = self.dbconn.cursor()
        if isinstance(self.dbconn, SQLiteDBConn):
            cur.execute('SELECT * FROM hashmap WHERE hash = ?', [checksum])
        elif isinstance(self.dbconn, PostgresDBConn):
            cur.execute('SELECT * FROM hashmap WHERE hash = %s', [checksum])
        res = cur.fetchone()

        if res is None or res is False:
            return False

        return True

    def get_md5(self, path, f):
        ''' get md5 hash from the first kilobyte of data '''
        # skip header row in CSV format(~1.6kb)
        if path[-4:].lower() == '.csv':
            _ = f.read(1600)
        digest = md5(f.read(1000)).hexdigest()
        return digest


def _fast_unzip(zipf, dirname):
    ''' parallel process worker for fast_unzip() '''
    if zipf.lower()[-4:] == '.zip':
        exists = set(sorted(os.listdir(dirname)))
        with zipfile.ZipFile(zipf, 'r') as zip_ref:
            contents = set(zip_ref.namelist())
            members = list(contents - exists)
            zip_ref.extractall(path=dirname, members=members)
    elif zipf.lower()[-3:] == '.gz':
        unzip_file = os.path.join(dirname,
                                  zipf.rsplit(os.path.sep, 1)[-1][:-3])
        with gzip.open(zipf, 'rb') as f1, open(unzip_file, 'wb') as f2:
            f2.write(f1.read())
    else:
        raise ValueError('unknown zip file type')


def fast_unzip(zipfilenames, dirname, processes=12):
    ''' unzip many files in parallel
        any existing unzipped files in the target directory will be skipped
    '''

    print(f'unzipping files to {dirname} ... '
          '(set the TMPDIR environment variable to change this)')

    fcn = partial(_fast_unzip, dirname=dirname)
    '''
    with Pool(processes) as p:
        p.imap_unordered(fcn, zipfilenames)
        p.close()
        p.join()
    '''
    for file in zipfilenames:
        fcn(file)


def decode_msgs(filepaths,
                dbconn,
                source,
                vacuum=False,
                skip_checksum=False,
                verbose=True):
    ''' Decode NMEA format AIS messages and store in an SQLite database.
        To speed up decoding, create the database on a different hard drive
        from where the raw data is stored.
        A checksum of the first kilobyte of every file will be stored to
        prevent loading the same file twice.

        If the filepath has a .gz or .zip extension, the file will be
        decompressed into a temporary directory before database insert.

        args:
            filepaths (list)
                absolute filepath locations for AIS message files to be
                ingested into the database
            dbconn (:class:`aisdb.database.dbconn.DBConn`)
                database connection object
            source (string)
                data source name or description. will be used as a primary key
                column, so duplicate messages from different sources will not
                be ignored as duplicates upon insert
            vacuum (boolean, str)
                if True, the database will be vacuumed after completion.
                if string, the database will be vacuumed into the filepath
                given. Consider vacuuming to second hard disk to speed this up

        returns:
            None

        example:


        .. _example_decode:

        >>> import os
        >>> from aisdb import decode_msgs, DBConn
        >>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv',
        ...              'aisdb/tests/testdata/test_data_20211101.nm4']
        >>> with SQLiteDBConn('test_decode_msgs.db') as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn,
        ...                 source='TESTING', verbose=False)
    '''
    '''
        >>> os.remove('test_decode_msgs.db')
    '''
    #        psql_conn_string (string)
    #            Postgres connection string. If dbconn is an SQLite database
    #            connection, set this to ``None``.
    if not isinstance(dbconn,
                      (SQLiteDBConn, PostgresDBConn)):  # pragma: no cover
        raise ValueError('db argument must be a DBConn database connection. '
                         f'got {dbconn}')

    if len(filepaths) == 0:  # pragma: no cover
        raise ValueError('must supply atleast one filepath.')

    dbindex = FileChecksums(dbconn=dbconn)

    # handle zipfiles
    zipped = {
        f
        for f in filepaths
        if f.lower()[-4:] == '.zip' or f.lower()[-3:] == '.gz'
    }
    not_zipped = sorted(list(set(filepaths) - set(zipped)))
    zipped_checksums = []
    not_zipped_checksums = []
    unzipped_checksums = []
    _skipped = []

    if verbose:
        print('generating file checksums...')

    for item in deepcopy(zipped):
        with open(os.path.abspath(item), 'rb') as f:
            signature = dbindex.get_md5(item, f)
        if skip_checksum:
            continue
        if dbindex.checksum_exists(signature):
            _skipped.append(item)
            zipped.remove(item)
            if verbose:
                print(f'found matching checksum, skipping {item}')
        else:
            zipped_checksums.append(signature)

    for item in deepcopy(not_zipped):
        with open(os.path.abspath(item), 'rb') as f:
            signature = dbindex.get_md5(item, f)
        if skip_checksum:
            continue
        if dbindex.checksum_exists(signature):
            _skipped.append(item)
            not_zipped.remove(item)
            if verbose:
                print(f'found matching checksum, skipping {item}')
        else:
            not_zipped_checksums.append(signature)

    if zipped:
        fast_unzip(zipped, dbindex.tmp_dir)
        unzipped = sorted([
            os.path.join(dbindex.tmp_dir, f)
            for f in os.listdir(dbindex.tmp_dir)
        ])
        assert unzipped
        if not skip_checksum:
            for item in unzipped:
                with open(os.path.abspath(item), 'rb') as f:
                    signature = dbindex.get_md5(item, f)
                unzipped_checksums.append(signature)
    else:
        unzipped = []

    raw_files = not_zipped + unzipped
    raw_files

    if not raw_files:
        print('All files returned an existing checksum.',
              'Cleaning temporary data...')
        for tmpfile in unzipped:
            os.remove(tmpfile)
        os.removedirs(dbindex.tmp_dir)
        return

    assert skip_checksum or len(not_zipped) == len(not_zipped_checksums)
    assert skip_checksum or len(zipped) == len(zipped_checksums)
    assert skip_checksum or len(unzipped) == len(unzipped_checksums)

    # TODO: get file dates and create new tables before insert
    if verbose:
        print('checking file dates...')
    filedates = [getfiledate(f) for f in raw_files]
    months = [
        month.strftime('%Y%m') for month in rrule(
            freq=MONTHLY,
            dtstart=min(filedates) - (timedelta(days=min(filedates).day - 1)),
            until=max(filedates),
        )
    ]

    if verbose:
        print('creating tables and dropping table indexes...')

    # drop constraints and indexes to speed up insert,
    # and rebuild them after inserting
    if isinstance(dbconn, PostgresDBConn):
        with open(
                os.path.join(sqlpath, 'psql_createtable_dynamic_noindex.sql'),
                'r') as f:
            create_dynamic_table_stmt = f.read()
        with open(os.path.join(sqlpath, 'createtable_static.sql'), 'r') as f:
            create_static_table_stmt = f.read()
        for month in months:
            dbconn.execute(create_dynamic_table_stmt.format(month))
            dbconn.execute(create_static_table_stmt.format(month))
            dbconn.execute(
                f'ALTER TABLE ais_{month}_dynamic '
                f'DROP CONSTRAINT IF EXISTS ais_{month}_dynamic_pkey')
            for idx_name in ('mmsi', 'time', 'lon', 'lat', 'cluster'):
                dbconn.execute(
                    f'DROP INDEX IF EXISTS idx_ais_{month}_dynamic_{idx_name}')
        dbconn.commit()
        completed_files = decoder(dbpath='',
                                  psql_conn_string=dbconn.connection_string,
                                  files=raw_files,
                                  source=source,
                                  verbose=verbose)

    elif isinstance(dbconn, SQLiteDBConn):
        with open(os.path.join(sqlpath, 'createtable_dynamic_clustered.sql'),
                  'r') as f:
            create_table_stmt = f.read()
        for month in months:
            dbconn.execute(create_table_stmt.format(month))
        completed_files = decoder(dbpath=dbconn.dbpath,
                                  psql_conn_string='',
                                  files=raw_files,
                                  source=source,
                                  verbose=verbose)
    else:
        assert False

    if verbose and not skip_checksum:
        print('saving checksums...')

    for filename, signature in zip(not_zipped + unzipped,
                                   not_zipped_checksums + unzipped_checksums):
        if filename in completed_files:
            dbindex.insert_checksum(signature)
        else:
            if verbose:
                print(f'error processing {filename}, skipping checksum...')

    dbindex.dbconn.commit()

    if verbose:
        print('cleaning temporary data...')

    for tmpfile in unzipped:
        os.remove(tmpfile)
    os.removedirs(dbindex.tmp_dir)

    if isinstance(dbconn, PostgresDBConn):
        if verbose:
            print('rebuilding indexes...')
        for month in months:
            dbconn.rebuild_indexes(month, verbose)
        dbconn.execute('ANALYZE')
        dbconn.commit()

    dbconn.aggregate_static_msgs(months, verbose)

    if vacuum is not False:
        print("finished parsing data\nvacuuming...")
        if isinstance(dbconn, SQLiteDBConn):
            if vacuum is True:
                dbconn.execute('VACUUM')
            elif isinstance(vacuum, str):
                assert not os.path.isfile(vacuum)
                dbconn.execute(f"VACUUM INTO '{vacuum}'")
            else:
                raise ValueError(
                    'vacuum arg must be boolean or filepath string')
            dbconn.commit()
        elif isinstance(dbconn, (PostgresDBConn, psycopg.Connection)):
            pass
        else:
            raise RuntimeError
        '''
            if vacuum is True:
                dbconn.commit()
                previous = dbconn.conn.autocommit
                dbconn.conn.autocommit = True
                dbconn.execute(
                    'VACUUM (verbose, index_cleanup, analyze)')
                dbconn.conn.autocommit = previous
            elif isinstance(vacuum, str):
                raise ValueError(
                    'vacuum parameter must be True or False for PostgresDBConn'
                )
            else:
                assert vacuum is False
            '''

    return

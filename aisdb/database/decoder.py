''' Parsing NMEA messages to create an SQL database.
    See function decode_msgs() for usage
'''

from hashlib import md5
import gzip
import os
import pickle
import sqlite3
import tempfile
import zipfile

from aisdb.database.dbconn import SQLiteDBConn, PostgresDBConn
from aisdb.aisdb import decoder


class FileChecksums():

    def __init__(self, *, dbconn):
        assert isinstance(dbconn, (PostgresDBConn, SQLiteDBConn))
        if isinstance(dbconn, SQLiteDBConn):
            assert len(dbconn.dbpaths) == 1, f'{dbconn.dbpaths}'
        self.dbconn = dbconn
        self.checksums_table()
        if not os.path.isdir(
                '/tmp') and os.name == 'posix':  # pragma: no cover
            os.mkdir('/tmp')
        self.tmp_dir = tempfile.mkdtemp()

    def checksums_table(self):
        ''' instantiates new database connection and creates a checksums
            hashmap table if it doesn't exist yet.

            creates a temporary directory with a path stored in ``self.tmp_dir``

            creates SQLite connection attribute ``self.dbconn``, which should
            be closed after use

            e.g.
                self.dbconn.close()
        '''
        # self.dbconn = sqlite3.connect(self.dbpath)
        if isinstance(self.dbconn, SQLiteDBConn):
            dbconn = sqlite3.connect(self.dbconn.dbpaths[0])
            cur = dbconn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS
                hashmap(
                    hash INTEGER PRIMARY KEY,
                    bytes BLOB
                )
                WITHOUT ROWID;''')
            cur.execute('CREATE UNIQUE INDEX '
                        'IF NOT EXISTS '
                        'idx_map on hashmap(hash)')
            dbconn.close()
        elif isinstance(self.dbconn, PostgresDBConn):
            dbconn = self.dbconn
            cur = self.dbconn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS
                hashmap(
                    hash TEXT PRIMARY KEY,
                    bytes BYTEA
                );''')
            cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS '
                        'idx_map on hashmap(hash);')

    def insert_checksum(self, checksum):
        if isinstance(self.dbconn, SQLiteDBConn):
            dbconn = sqlite3.connect(self.dbconn.dbpaths[0])
            dbconn.execute('INSERT INTO hashmap VALUES (?,?)',
                           [checksum, pickle.dumps(None)])
        elif isinstance(self.dbconn, PostgresDBConn):
            dbconn = self.dbconn
            dbconn.execute('INSERT INTO hashmap VALUES ($1,$2)',
                           [checksum, pickle.dumps(None)])
        dbconn.commit()
        if isinstance(self.dbconn, SQLiteDBConn):
            dbconn.close()

    def checksum_exists(self, checksum):
        # dbconn = sqlite3.connect(self.dbpath)
        # cur = dbconn.cursor()
        if isinstance(self.dbconn, SQLiteDBConn):
            dbconn = sqlite3.connect(self.dbconn.dbpaths[0])
            cur = dbconn.cursor()
            cur.execute('SELECT * FROM hashmap WHERE hash = ?', [checksum])
        elif isinstance(self.dbconn, PostgresDBConn):
            dbconn = self.dbconn
            cur = self.dbconn.cursor()
            cur.execute('SELECT * FROM hashmap WHERE hash = %s', [checksum])
        res = cur.fetchone()
        dbconn.commit()
        # dbconn.close()
        if isinstance(self.dbconn, SQLiteDBConn):
            dbconn.close()

        if res is None or res is False:
            return False
        return True

    def get_md5(self, path, f):
        ''' get md5 hash from the first kilobyte of data '''
        # skip header row in CSV format(~1.6kb)
        if path[-4:].lower() == '.csv':
            _ = f.read(1600)
        signature = md5(f.read(1000)).hexdigest()
        return signature


def _decode_gz(file, tmp_dir, dbpath, psql_conn_string, source, verbose):
    if dbpath is None:  # pragma: no cover
        dbpath = ''
    if psql_conn_string is None:  # pragma: no cover
        psql_conn_string = ''
    unzip_file = os.path.join(tmp_dir, file.rsplit(os.path.sep, 1)[-1][:-3])
    with gzip.open(file, 'rb') as f1, open(unzip_file, 'wb') as f2:
        f2.write(f1.read())
    decoder(dbpath=dbpath,
            psql_conn_string=psql_conn_string,
            files=[unzip_file],
            source=source,
            verbose=verbose)
    os.remove(unzip_file)


def _decode_ziparchive(file, tmp_dir, dbpath, psql_conn_string, source,
                       verbose):
    if dbpath is None:  # pragma: no cover
        dbpath = ''
    if psql_conn_string is None:  # pragma: no cover
        psql_conn_string = ''
    zipf = zipfile.ZipFile(file)
    for item in zipf.namelist():
        unzip_file = os.path.join(tmp_dir, item)
        with zipf.open(item, 'r') as f1, open(unzip_file, 'wb') as f2:
            f2.write(f1.read())
        decoder(dbpath=dbpath,
                psql_conn_string=psql_conn_string,
                files=[unzip_file],
                source=source,
                verbose=verbose)
        os.remove(unzip_file)
    zipf.close()


def decode_msgs(
        filepaths,
        dbconn,
        source,
        dbpath=None,
        #psql_conn_string=None,
        vacuum=False,
        skip_checksum=False,
        verbose=False):
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
            dbpath (string)
                SQLite database filepath to store results in. If dbconn is a
                Postgres database connection, set this to ``None``.
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

        >>> dbpath = 'test_decode_msgs.db'
        >>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv',
        ...              'aisdb/tests/testdata/test_data_20211101.nm4']
        >>> with DBConn() as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn, dbpath=dbpath, source='TESTING')
        >>> os.remove(dbpath)
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

    if isinstance(dbconn, SQLiteDBConn):
        dbconn._attach(dbpath)
        assert dbpath is not None
        #assert psql_conn_string is None
        psql_conn_string = ''
    else:
        #assert psql_conn_string is not None
        psql_conn_string = dbconn.connection_string
        assert dbpath is None
        dbpath = ''

    dbindex = FileChecksums(dbconn=dbconn)
    for file in filepaths:
        if not skip_checksum:
            with open(os.path.abspath(file), 'rb') as f:
                signature = dbindex.get_md5(file, f)
            if dbindex.checksum_exists(signature):
                if verbose:  # pragma: no cover
                    print(f'found matching checksum, skipping {file}')
                continue
        if file[-3:] == '.gz':
            _decode_gz(file,
                       dbindex.tmp_dir,
                       source=source,
                       verbose=verbose,
                       psql_conn_string=psql_conn_string,
                       dbpath=dbpath)
        elif file[-4:] == '.zip':
            _decode_ziparchive(file,
                               dbindex.tmp_dir,
                               source=source,
                               verbose=verbose,
                               psql_conn_string=psql_conn_string,
                               dbpath=dbpath)
        else:
            decoder(dbpath=dbpath,
                    psql_conn_string=psql_conn_string,
                    files=[file],
                    source=source,
                    verbose=verbose)
        if not skip_checksum:
            dbindex.insert_checksum(signature)
    os.removedirs(dbindex.tmp_dir)

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
        elif isinstance(dbconn, PostgresDBConn):
            if vacuum is True:
                previous = dbconn.conn.autocommit
                dbconn.conn.autocommit = True
                dbconn.execute('VACUUM')
                dbconn.conn.autocommit = previous
            elif isinstance(vacuum, str):
                raise ValueError(
                    'vacuum parameter must be True or False for PostgresDBConn'
                )
            else:
                assert vacuum is False
        else:
            raise RuntimeError

    return

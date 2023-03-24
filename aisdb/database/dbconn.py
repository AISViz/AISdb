''' SQLite Database connection

    Also see: https://docs.python.org/3/library/sqlite3.html#connection-objects
'''

from calendar import monthrange
from datetime import datetime
import os
import re
import warnings

from aisdb import sqlite3, sqlpath

import psycopg


class _DBConn():
    ''' AISDB Database connection handler '''

    def __enter__(self):
        return self

    def __exit__(self, exc_class, exc, tb):
        #cur = self.cursor()
        try:
            for dbpath in self.dbpaths:
                self.execute('DETACH DATABASE ?', [self._get_dbname(dbpath)])
            #cur.close()
        except Exception:
            print('rolling back...')
            self.rollback()
        finally:
            self.close()
        self = None

    def _create_table_coarsetype(self):
        ''' create a table to describe integer vessel type as a human-readable
            string.
        '''
        with open(os.path.join(sqlpath, 'coarsetype.sql'), 'r') as f:
            coarsetype_sql = f.read().split(';')
        cur = self.cursor()
        for row in coarsetype_sql:
            if row == '\n':
                continue
            #cur.execute(row)
            self.execute(row)
        self.commit()
        cur.close()

    def _get_dbname(self, dbpath):
        name_ext = os.path.split(dbpath)[1]
        name = name_ext.split('.')[0]
        return name

    def _attach(self, dbpath):
        ''' connect to an additional database file '''
        assert self._get_dbname(dbpath) != 'main'
        #cur = self.cursor()

        if dbpath not in self.dbpaths:
            #cur.execute('ATTACH DATABASE ? AS ?',
            self.execute('ATTACH DATABASE ? AS ?',
                    [dbpath, self._get_dbname(dbpath)])
            self.dbpaths.append(dbpath)
        #cur.close()

        # query the temporal range of monthly database tables
        # results will be stored as a dictionary attribute db_daterange
        #cur = self.cursor()
        sql_qry = (
                f'SELECT * FROM {self._get_dbname(dbpath)}.sqlite_master '
                'WHERE type="table" AND name LIKE "ais\\_%\\_dynamic" ESCAPE "\\" '
                )
        try:
            cur = self.cursor()
            cur.execute(sql_qry)
            dynamic_tables = cur.fetchall()
            if dynamic_tables != []:
                db_months = sorted(
                        [table['name'].split('_')[1] for table in dynamic_tables])
                self.db_daterange[self._get_dbname(dbpath)] = {
                        'start':
                        datetime(int(db_months[0][:4]), int(db_months[0][4:]),
                            1).date(),
                        'end':
                        datetime((y := int(db_months[-1][:4])),
                            (m := int(db_months[-1][4:])),
                            monthrange(y, m)[1]).date(),
                        }
        except Exception as err:
            warnings.warn(str(err.with_traceback(None)))
        finally:
            cur.close()


class SQLiteDBConn(_DBConn, sqlite3.Connection):
    ''' SQLite3 database connection object

        attributes:
            dbpaths (list of strings)
                list of currently attached databases
            db_daterange (dict)
                temporal range of monthly database tables. keys are DB file
                names
    '''

    def __init__(self):
        # configs
        self.dbpaths = []
        self.db_daterange = {}
        super().__init__(':memory:',
                timeout=5,
                detect_types=sqlite3.PARSE_DECLTYPES
                | sqlite3.PARSE_COLNAMES)
        self.row_factory = sqlite3.Row
        '''
            cur = self.cursor()
            cur.execute("PRAGMA journal_mode")
            res = cur.fetchone()[0]
            if res != 'wal':
                self.execute('PRAGMA journal_mode=wal')
                self.commit()
        '''
        self._create_table_coarsetype()


# default to local SQLite database
DBConn = SQLiteDBConn


class PostgresDBConn(_DBConn, psycopg.Connection):
    ''' This feature requires the optional dependency psycopg for interfacing Postgres
        databases.

        The following keyword arguments are accepted by Postgres:
        | https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS

        Alternatively, a connection string may be used.
        Information on connection strings and postgres URI format can be found here:
        | https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

        Example:

        .. code-block:: python

            import os
            from aisdb.database.dbconn import PostgresDBConn
            # keyword arguments
            dbconn = PostgresDBConn(
                hostaddr='127.0.0.1',
                user='postgres',
                port=5432,
                password=os.environ.get('POSTGRES_PASSWORD'),
            )
            # libpq connection string
            dbconn = PostgresDBConn('Postgresql://localhost:5433')

    '''

    def __init__(self, libpq_connstring=None, **kwargs):
        if libpq_connstring is not None:
            self.conn = psycopg.connect(libpq_connstring)
        else:
            self.conn = psycopg.connect(**kwargs)
        self.cursor = self.conn.cursor
        self.commit = self.conn.commit
        self.rollback = self.conn.rollback
        self.close = self.conn.close
        self.__repr__ = self.conn.__repr__
        #conn = psycopg.connect(conninfo=libpq_connstring)
        self.pgconn = self.conn.pgconn
        #self = conn

        #self.dbpaths = []
        self.db_daterange = {}


    def execute(self, sql, args=[]):
        sql = re.sub(r'\$[0-9][0-9]*', r'%s', sql)
        with self.cursor() as cur:
            cur.execute(sql, args)

''' SQLite Database connection

    Also see: https://docs.python.org/3/library/sqlite3.html#connection-objects
'''

from calendar import monthrange
from datetime import datetime
from enum import Enum
import os
import re
import warnings
import ipaddress

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
        #cur = self.cursor()
        for stmt in coarsetype_sql:
            if stmt == '\n':
                continue
            #cur.execute(stmt)
            self.execute(stmt)
        self.commit()
        #cur.close()


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
        self._create_table_coarsetype()

    def _get_dbname(self, dbpath):
        name_ext = os.path.split(dbpath)[1]
        name = name_ext.split('.')[0]
        return name

    def _attach(self, dbpath):
        ''' connect to an additional database file '''
        assert dbpath is not None
        dbname = self._get_dbname(dbpath)

        assert dbname is not None
        assert dbname != 'main'
        assert dbname != 'temp'
        assert dbname != ''

        if dbpath not in self.dbpaths:
            if os.environ.get('DEBUG'):
                print('attaching database:', dbpath, dbname)
            try:
                self.execute('ATTACH ? AS ?', [dbpath, dbname])
            except Exception as e:
                print(f'failed: ATTACH {dbpath} AS {dbname}')
                raise e
            self.dbpaths.append(dbpath)
        #cur.close()

        # check if the database contains marinetraffic data
        sql_qry_traffictable = (
            f'SELECT * FROM {dbname}.sqlite_master '
            'WHERE type="table" AND name = "webdata_marinetraffic"')
        cur = self.execute(sql_qry_traffictable)
        if len(cur.fetchall()) > 0:
            self.trafficdb = dbpath
        cur.close()

        # query the temporal range of monthly database tables
        # results will be stored as a dictionary attribute db_daterange
        #cur = self.cursor()
        sql_qry = (
            f'SELECT * FROM {dbname}.sqlite_master '
            'WHERE type="table" AND name LIKE "ais\\_%\\_dynamic" ESCAPE "\\" '
        )
        try:
            cur = self.cursor()
            cur.execute(sql_qry)
            dynamic_tables = cur.fetchall()
            if dynamic_tables != []:
                db_months = sorted(
                    [table['name'].split('_')[1] for table in dynamic_tables])
                self.db_daterange[dbname] = {
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

            # Alternatively, connect using a connection string:
            dbconn = PostgresDBConn('Postgresql://localhost:5433')

    '''

    def __init__(self, libpq_connstring=None, **kwargs):

        # store the connection string as an attribute
        # this info will be passed to rust when possible
        if libpq_connstring is not None:
            self.conn = psycopg.connect(libpq_connstring)
            self.connection_string = libpq_connstring
        else:
            self.conn = psycopg.connect(**kwargs)
            self.connection_string = 'postgresql://'

            if 'user' in kwargs.keys():
                self.connection_string += kwargs.pop('user')
            else:
                self.connection_string += 'postgres'

            if 'password' in kwargs.keys():
                self.connection_string += ':'
                self.connection_string += kwargs.pop('password')
            self.connection_string += '@'

            if 'hostaddr' in kwargs.keys():
                ip = ipaddress.ip_address(kwargs.pop('hostaddr'))
                if ip.version == 4:
                    self.connection_string += str(ip)
                elif ip.version == 6:
                    self.connection_string += '['
                    self.connection_string += str(ip)
                    self.connection_string += ']'
                else:
                    raise ValueError(str(ip))
            else:
                self.connection_string += 'localhost'
            self.connection_string += ':'

            if 'port' in kwargs.keys():
                self.connection_string += str(kwargs.pop('port'))
            else:
                self.connection_string += '5432'

            if 'dbname' in kwargs.keys():
                self.connection_string += '/'
                self.connection_string += kwargs.pop('dbname')

            if len(kwargs) > 0:
                self.connection_string += '?'
                for key, val in kwargs.items():
                    self.connection_string += f'{key}={val}&'
                self.connection_string = self.connection_string[:-1]

        self.cursor = self.conn.cursor
        self.commit = self.conn.commit
        self.rollback = self.conn.rollback
        self.close = self.conn.close
        self.__repr__ = self.conn.__repr__
        #conn = psycopg.connect(conninfo=libpq_connstring)
        self.pgconn = self.conn.pgconn
        #self = conn

        #self.dbpaths = []
        cur = self.cursor()

        coarsetype_qry = ("select table_name from information_schema.tables "
                          "where table_name = 'coarsetype_ref'")

        cur.execute(coarsetype_qry)
        coarsetype_exists = cur.fetchone()

        if not coarsetype_exists:
            self._create_table_coarsetype()

        dynamic_tables_qry = (
            "select table_name from information_schema.tables "
            "where table_name LIKE 'ais\_______\_dynamic' ORDER BY table_name")
        cur.execute(dynamic_tables_qry)
        res = cur.fetchall()

        if not res:
            self.db_daterange = {}
        else:
            first = res[0][0]
            last = res[-1][0]

            min_qry = f'SELECT MIN(time) FROM {first}'
            cur.execute(min_qry)
            min_time = datetime.utcfromtimestamp(cur.fetchone()[0])

            max_qry = f'SELECT MAX(time) FROM {last}'
            cur.execute(max_qry)
            max_time = datetime.utcfromtimestamp(cur.fetchone()[0])

            self.db_daterange = {'main': {'start': min_time, 'end': max_time}}

    def execute(self, sql, args=[]):
        sql = re.sub(r'\$[0-9][0-9]*', r'%s', sql)
        with self.cursor() as cur:
            cur.execute(sql, args)


class ConnectionType(Enum):
    ''' database connection types enum. used for static type hints '''
    SQLITE = SQLiteDBConn
    POSTGRES = PostgresDBConn

''' SQLite Database connection

    Also see: https://docs.python.org/3/library/sqlite3.html#connection-objects
'''

from calendar import monthrange
from collections import Counter
from datetime import datetime
from enum import Enum
import ipaddress
import os
import re
import warnings

from aisdb import sqlite3, sqlpath
from aisdb.database.create_tables import (
    sql_aggregate,
    sql_createtable_static,
)

import numpy as np
import psycopg

with open(os.path.join(sqlpath, 'coarsetype.sql'), 'r') as f:
    coarsetype_sql = f.read().split(';')


class _DBConn():
    ''' AISDB Database connection handler '''

    def _create_table_coarsetype(self):
        ''' create a table to describe integer vessel type as a human-readable
            string.
        '''
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
            dbpath (str)
                database filepath
            db_daterange (dict)
                temporal range of monthly database tables. keys are DB file
                names
    '''

    def __init__(self, dbpath):
        super().__init__(
            dbpath,
            timeout=5,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        self.dbpath = dbpath
        self.row_factory = sqlite3.Row
        coarsetype_exists_qry = (
            'SELECT * FROM sqlite_master '
            r'WHERE type="table" AND name LIKE "coarsetype_ref" ')
        cur = self.cursor()
        cur.execute(coarsetype_exists_qry)
        if len(cur.fetchall()) == 0:
            self._create_table_coarsetype()
        self._set_db_daterange()

    def _set_db_daterange(self):
        # query the temporal range of monthly database tables
        # results will be stored as a dictionary attribute db_daterange
        sql_qry = ('SELECT * FROM sqlite_master '
                   r'WHERE type="table" AND name LIKE "ais_%_dynamic" ')
        cur = self.cursor()
        cur.execute(sql_qry)
        dynamic_tables = cur.fetchall()
        if dynamic_tables != []:
            db_months = sorted(
                [table['name'].split('_')[1] for table in dynamic_tables])
            self.db_daterange = {
                'start':
                datetime(int(db_months[0][:4]), int(db_months[0][4:]),
                         1).date(),
                'end':
                datetime((y := int(db_months[-1][:4])),
                         (m := int(db_months[-1][4:])),
                         monthrange(y, m)[1]).date(),
            }
        else:
            self.db_daterange = {}
        cur.close()

    def aggregate_static_msgs(self, months_str: list, verbose: bool = True):
        ''' collect an aggregate of static vessel reports for each unique MMSI
            identifier. The most frequently repeated values for each MMSI will
            be kept when multiple different reports appear for the same MMSI

            this function should be called every time data is added to the database

            args:
                dbconn (:class:`aisdb.database.dbconn.SQLiteDBConn`)
                    database connection object
                months_str (list)
                    list of strings with format: YYYYmm
                verbose (bool)
                    logs messages to stdout
        '''

        assert hasattr(self, 'dbpath')
        assert not hasattr(self, 'dbpaths')

        cur = self.cursor()

        for month in months_str:
            # check for monthly tables in dbfiles containing static reports
            cur.execute(
                'SELECT name FROM sqlite_master '
                'WHERE type="table" AND name=?', [f'ais_{month}_static'])
            if cur.fetchall() == []:
                continue

            cur.execute(sql_createtable_static.format(month))

            if verbose:
                print('aggregating static reports into '
                      f'static_{month}_aggregate...')
            cur.execute('SELECT DISTINCT s.mmsi FROM '
                        f'ais_{month}_static AS s')
            mmsis = np.array(cur.fetchall(), dtype=int).flatten()

            cur.execute('DROP TABLE IF EXISTS '
                        f'static_{month}_aggregate')

            sql_select = '''
              SELECT
                s.mmsi, s.imo, TRIM(vessel_name) as vessel_name, s.ship_type,
                s.call_sign, s.dim_bow, s.dim_stern, s.dim_port, s.dim_star,
                s.draught
              FROM ais_{}_static AS s WHERE s.mmsi = ?
            '''.format(month)

            agg_rows = []
            for mmsi in mmsis:
                _ = cur.execute(sql_select, (str(mmsi), ))
                cur_mmsi = cur.fetchall()

                cols = np.array(cur_mmsi, dtype=object).T
                assert len(cols) > 0

                filtercols = np.array(
                    [
                        np.array(list(filter(None, col)), dtype=object)
                        for col in cols
                    ],
                    dtype=object,
                )

                paddedcols = np.array(
                    [col if len(col) > 0 else [None] for col in filtercols],
                    dtype=object,
                )

                aggregated = [
                    Counter(col).most_common(1)[0][0] for col in paddedcols
                ]

                agg_rows.append(aggregated)

            cur.execute(
                sql_aggregate.format(month).replace(
                    f'static_{month}_aggregate', f'static_{month}_aggregate'))

            if len(agg_rows) == 0:
                warnings.warn('no rows to aggregate! '
                              f'table: static_{month}_aggregate')
                continue

            skip_nommsi = np.array(agg_rows, dtype=object)
            assert len(skip_nommsi.shape) == 2
            skip_nommsi = skip_nommsi[skip_nommsi[:, 0] != None]
            assert len(skip_nommsi) > 1
            cur.executemany((
                f'INSERT INTO static_{month}_aggregate '
                f"VALUES ({','.join(['?' for _ in range(skip_nommsi.shape[1])])}) "
            ), skip_nommsi)

            self.commit()


# default to local SQLite database
DBConn = SQLiteDBConn


class PostgresDBConn(_DBConn, psycopg.Connection):
    ''' This feature requires optional dependency psycopg for interfacing
        Postgres databases.

        The following keyword arguments are accepted by Postgres:
        | https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS

        Alternatively, a connection string may be used.
        Information on connection strings and postgres URI format can be found
        here:
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
                dbname='postgres',
            )

            # Alternatively, connect using a connection string:
            dbconn = PostgresDBConn('Postgresql://localhost:5433')

    '''

    def _set_db_daterange(self):

        dynamic_tables_qry = psycopg.sql.SQL(
            "select table_name from information_schema.tables "
            r"where table_name LIKE 'ais\_______\_dynamic' ORDER BY table_name"
        )
        cur = self.cursor()
        cur.execute(dynamic_tables_qry)
        dynamic_tables = cur.fetchall()

        if dynamic_tables != []:
            db_months = sorted([
                table['table_name'].split('_')[1] for table in dynamic_tables
            ])
            self.db_daterange = {
                'start':
                datetime(int(db_months[0][:4]), int(db_months[0][4:]),
                         1).date(),
                'end':
                datetime((y := int(db_months[-1][:4])),
                         (m := int(db_months[-1][4:])),
                         monthrange(y, m)[1]).date(),
            }
        else:
            self.db_daterange = {}

    def __enter__(self):
        self.conn.__enter__()
        return self

    def __exit__(self, exc_class, exc, tb):
        self.conn.__exit__(exc_class, exc, tb)
        if exc_class or exc or tb:
            print('rolling back...')
            raise exc

    def __init__(self, libpq_connstring=None, **kwargs):

        # store the connection string as an attribute
        # this info will be passed to rust when possible
        if libpq_connstring is not None:
            self.conn = psycopg.connect(libpq_connstring,
                                        row_factory=psycopg.rows.dict_row)
            self.connection_string = libpq_connstring
        else:
            self.conn = psycopg.connect(row_factory=psycopg.rows.dict_row,
                                        **kwargs)
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
        self._adapters = self.conn.adapters

        cur = self.cursor()

        coarsetype_qry = ("select table_name from information_schema.tables "
                          "where table_name = 'coarsetype_ref'")

        cur.execute(coarsetype_qry)
        coarsetype_exists = cur.fetchone()

        if not coarsetype_exists:
            self._create_table_coarsetype()

        self._set_db_daterange()

    def execute(self, sql, args=[]):
        sql = re.sub(r'\$[0-9][0-9]*', r'%s', sql)
        with self.cursor() as cur:
            cur.execute(sql, args)

    def rebuild_indexes(self, month, verbose=True):
        if verbose:
            print(f'indexing {month}...')
        dbconn = self.conn
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_dynamic_mmsi '
            f'ON ais_{month}_dynamic (mmsi)')
        dbconn.commit()
        if verbose:
            print(f'done indexing mmsi: {month}')
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_dynamic_time '
            f'ON ais_{month}_dynamic (time)')
        dbconn.commit()
        if verbose:
            print(f'done indexing time: {month}')
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_dynamic_lon '
            f'ON ais_{month}_dynamic (longitude)')
        dbconn.commit()
        if verbose:
            print(f'done indexing longitude: {month}')
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_dynamic_lat '
            f'ON ais_{month}_dynamic (latitude)')
        dbconn.commit()
        if verbose:
            print(f'done indexing latitude: {month}')
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_dynamic_cluster '
            f'ON ais_{month}_dynamic (mmsi, time, longitude, latitude, source)'
        )
        dbconn.commit()
        if verbose:
            print(f'done indexing combined index: {month}')
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_static_mmsi '
            f'ON ais_{month}_static (mmsi)')
        dbconn.commit()
        if verbose:
            print(f'done indexing static mmsi: {month}')
        dbconn.execute(
            f'CREATE INDEX IF NOT EXISTS idx_ais_{month}_static_time '
            f'ON ais_{month}_static (time)')
        dbconn.commit()
        if verbose:
            print(f'done indexing static time: {month}')
        dbconn.execute(
            f'CLUSTER {"VERBOSE" if verbose else ""} ais_{month}_dynamic\n'
            f'USING idx_ais_{month}_dynamic_cluster')
        dbconn.commit()
        if verbose:
            print(f'done clustering: {month}')

    def deduplicate_dynamic_msgs(self, month: str, verbose=True):
        dbconn = self.conn
        dbconn.execute(f'''
            DELETE FROM ais_{month}_dynamic WHERE ctid IN
                (SELECT ctid FROM
                    (SELECT *, ctid, row_number() OVER
                        (PARTITION BY mmsi, time, source ORDER BY ctid)
                    FROM ais_{month}_dynamic ) AS duplicates_{month}
                WHERE row_number > 1)
            ''')
        dbconn.commit()
        if verbose:
            print(f'done deduplicating: {month}')

    def aggregate_static_msgs(self, months_str: list, verbose: bool = True):
        ''' collect an aggregate of static vessel reports for each unique MMSI
            identifier. The most frequently repeated values for each MMSI will
            be kept when multiple different reports appear for the same MMSI

            this function should be called every time data is added to the database

            args:
                months_str (list)
                    list of strings with format: YYYYmm
                verbose (bool)
                    logs messages to stdout
        '''

        cur = self.cursor()

        for month in months_str:
            # check for monthly tables in dbfiles containing static reports
            cur.execute('SELECT table_name FROM information_schema.tables '
                        f'WHERE table_name = \'ais_{month}_static\'')
            static_tables = cur.fetchall()
            if static_tables == []:
                continue

            if verbose:
                print('aggregating static reports into '
                      f'static_{month}_aggregate...')
            cur.execute(f'SELECT DISTINCT s.mmsi FROM ais_{month}_static AS s')
            mmsi_res = cur.fetchall()
            if mmsi_res == []:
                mmsis = np.array([], dtype=int)
            else:
                mmsis = np.array(sorted([r['mmsi'] for r in mmsi_res]),
                                 dtype=int).flatten()

            cur.execute(
                psycopg.sql.SQL(
                    f'DROP TABLE IF EXISTS static_{month}_aggregate'))

            sql_select = psycopg.sql.SQL(f'''
              SELECT
                s.mmsi, s.imo, TRIM(vessel_name) as vessel_name, s.ship_type,
                s.call_sign, s.dim_bow, s.dim_stern, s.dim_port, s.dim_star,
                s.draught
              FROM ais_{month}_static AS s WHERE s.mmsi = %s
            ''')

            agg_rows = []
            for mmsi in mmsis:
                _ = cur.execute(sql_select, (str(mmsi), ))
                cur_mmsi = [tuple(i.values()) for i in cur.fetchall()]
                cols = np.array(cur_mmsi, dtype=object).T
                assert len(cols) > 0

                filtercols = np.array(
                    [
                        np.array(list(filter(None, col)), dtype=object)
                        for col in cols
                    ],
                    dtype=object,
                )

                paddedcols = np.array(
                    [col if len(col) > 0 else [None] for col in filtercols],
                    dtype=object,
                )

                aggregated = [
                    Counter(col).most_common(1)[0][0] for col in paddedcols
                ]

                agg_rows.append(aggregated)

            cur.execute(sql_aggregate.format(month))

            if len(agg_rows) == 0:
                warnings.warn('no rows to aggregate! '
                              f'table: static_{month}_aggregate')
                return

            skip_nommsi = np.array(agg_rows, dtype=object)
            assert len(skip_nommsi.shape) == 2
            skip_nommsi = skip_nommsi[skip_nommsi[:, 0] != None]
            assert len(skip_nommsi) > 1
            insert_vals = ','.join(['%s' for _ in range(skip_nommsi.shape[1])])
            insert_stmt = psycopg.sql.SQL(
                f'INSERT INTO static_{month}_aggregate '
                f'VALUES ({insert_vals})')
            cur.executemany(insert_stmt, map(tuple, skip_nommsi))

            self.commit()


class ConnectionType(Enum):
    ''' database connection types enum. used for static type hints '''
    SQLITE = SQLiteDBConn
    POSTGRES = PostgresDBConn

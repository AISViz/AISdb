''' exposes the SQLite DB connection. some postgres code is also included for legacy support '''

import os

from common import dbpath, table_prefix


def create_table_coarsetype(cur):
    ''' create a table to describe integer vessel type as a human-readable string
        included here instead of create_tables.py to prevent circular import error
    '''

    cur.execute(''' CREATE TABLE IF NOT EXISTS coarsetype_ref (
            coarse_type integer,
            coarse_type_txt character varying(75)
        ); ''')

    cur.execute(
        ''' CREATE UNIQUE INDEX idx_coarsetype ON 'coarsetype_ref' (coarse_type)'''
    )

    cur.executemany(
        ''' INSERT OR IGNORE INTO coarsetype_ref (coarse_type, coarse_type_txt) VALUES (?,?) ''',
        (
            (20, 'Wing in ground craft'),
            (30, 'Fishing'),
            (31, 'Towing'),
            (32, 'Towing - length >200m or breadth >25m'),
            (33, 'Engaged in dredging or underwater operations'),
            (34, 'Engaged in diving operations'),
            (35, 'Engaged in military operations'),
            (36, 'Sailing'),
            (37, 'Pleasure craft'),
            (38, 'Reserved for future use'),
            (39, 'Reserved for future use'),
            (40, 'High speed craft'),
            (50, 'Pilot vessel'),
            (51, 'Search and rescue vessels'),
            (52, 'Tugs'),
            (53, 'Port tenders'),
            (54, 'Vessels with anti-pollution facilities or equipment'),
            (55, 'Law enforcement vessels'),
            (56, 'Spare for assignments to local vessels'),
            (57, 'Spare for assignments to local vessels'),
            (58, 'Medical transports (1949 Geneva convention)'),
            (59,
             'Ships and aircraft of States not parties to an armed conflict'),
            (60, 'Passenger ships'),
            (70, 'Cargo ships'),
            (80, 'Tankers'),
            (90, 'Other types of ship'),
            (100, 'Unknown'),
        ))


class DBConn():
    ''' class to return a database connection object

        by default this will create a new SQLite database if the dbpath does
        not yet exist. postgres code is also included for legacy purposes.

        attributes:
            conn (sqlite3.Connection)
                database connection object
            cur (sqlite3.Cursor)
                database cursor object
            lambdas (dict)
                included here instead of in lambdas.py to allow for
                legacy postgres support depending on which database type is used.
                see lambdas.py for more info
            dbtype (string)
                either 'sqlite3' or 'postgres' depending on which was used
    '''

    def __init__(self, dbpath=dbpath, postgres=False, timeout=5):
        if postgres or os.environ.get('POSTGRESDB'):
            import psycopg2
            import psycopg2.extras
            if __name__ == '__main__':
                psycopg2.extensions.set_wait_callback(
                    psycopg2.extras.wait_select)  # enable interrupt
            conn = psycopg2.connect(dbname='ee_ais',
                                    user=os.environ.get('PGUSER'),
                                    port=os.environ.get('PGPORT'),
                                    password=os.environ.get('PGPASS'),
                                    host='localhost')
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self.conn, self.cur = conn, cur
            self.dbtype = 'postgres'
            # create_table_coarsetype(self.cur)

        else:
            import sqlite3
            # import pysqlite3 as sqlite3
            self.dbtype = 'sqlite3'

            if dbpath is not None and dbpath != ':memory:':
                if not os.path.isdir(os.path.dirname(dbpath)):
                    print(f'creating directory path: {dbpath}')
                    os.mkdir(os.path.dirname(dbpath))
            else:
                dbpath = ':memory:'

            self.conn = sqlite3.connect(dbpath,
                                        timeout=timeout,
                                        detect_types=sqlite3.PARSE_DECLTYPES
                                        | sqlite3.PARSE_COLNAMES)

            self.conn.execute('PRAGMA synchronous=0')
            self.conn.execute('PRAGMA temp_store=MEMORY')
            self.conn.execute('PRAGMA threads=8')
            self.conn.commit()

            self.cur = self.conn.cursor()
            self.cur.execute(
                'SELECT name FROM sqlite_master WHERE type="table" AND name="coarsetype_ref";'
            )
            if not self.cur.fetchall():
                create_table_coarsetype(self.cur)
            self.conn.commit()

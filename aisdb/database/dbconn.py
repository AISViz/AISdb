''' exposes the SQLite DB connection. some postgres code is also included for legacy support '''

import os
import sqlite3

from common import dbpath


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


# TODO: refactor this with subclassing
class DBConn():
    ''' SQLite3 database connection object

        by default this will create a new SQLite database if the dbpath does
        not yet exist

        args:
            dbpath (string)
                defaults to dbpath as configured in ~/.config/ais.cfg

        attributes:
            conn (sqlite3.Connection)
                database connection object
            cur (sqlite3.Cursor)
                database cursor object
    '''

    def __init__(self, dbpath=dbpath):
        if dbpath is not None and dbpath != ':memory:':
            if not os.path.isdir(os.path.dirname(dbpath)):
                print(f'creating directory path: {dbpath}')
                os.mkdir(os.path.dirname(dbpath))
        else:
            dbpath = ':memory:'

        self.conn = sqlite3.connect(dbpath,
                                    timeout=5,
                                    detect_types=sqlite3.PARSE_DECLTYPES
                                    | sqlite3.PARSE_COLNAMES)

        self.conn.execute('PRAGMA synchronous=0')
        self.conn.execute('PRAGMA temp_store=MEMORY')
        self.conn.execute('PRAGMA threads=3')
        self.conn.commit()

        self.cur = self.conn.cursor()
        self.cur.execute(
            'SELECT name FROM sqlite_master WHERE type="table" AND name="coarsetype_ref";'
        )
        if not self.cur.fetchall():
            create_table_coarsetype(self.cur)
        self.conn.commit()

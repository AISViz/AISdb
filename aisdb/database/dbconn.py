''' exposes the SQLite DB connection. some postgres code is also included for legacy support '''

import os
import sqlite3
if (sqlite3.sqlite_version_info[0] < 3
        or (sqlite3.sqlite_version_info[0] <= 3
            and sqlite3.sqlite_version_info[1] < 35)):
    import pysqlite3 as sqlite3


def create_table_coarsetype(cur):
    ''' create a table to describe integer vessel type as a human-readable string
        included here instead of create_tables.py to prevent circular import error
    '''

    cur.execute(''' CREATE TABLE IF NOT EXISTS coarsetype_ref (
            coarse_type integer,
            coarse_type_txt character varying(75)
        ); ''')

    cur.execute(
        'CREATE UNIQUE INDEX idx_coarsetype ON coarsetype_ref(coarse_type)')

    cur.executemany(
        ''' INSERT OR IGNORE INTO coarsetype_ref (coarse_type, coarse_type_txt) VALUES (?,?) ''',
        (
            (20, 'Wing in ground craft'),
            (21, 'Wing in ground craft, hazardous category A'),
            (22, 'Wing in ground craft, hazardous category B'),
            (23, 'Wing in ground craft, hazardous category C'),
            (24, 'Wing in ground craft, hazardous category D'),
            (25, 'Wing in ground craft'),
            (26, 'Wing in ground craft'),
            (27, 'Wing in ground craft'),
            (28, 'Wing in ground craft'),
            (29, 'Wing in ground craft'),
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
            (41, 'High speed craft, hazardous category A'),
            (42, 'High speed craft, hazardous category B'),
            (43, 'High speed craft, hazardous category C'),
            (44, 'High speed craft, hazardous category D'),
            (45, 'High speed craft'),
            (46, 'High speed craft'),
            (47, 'High speed craft'),
            (48, 'High speed craft'),
            (49, 'High speed craft'),
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
            (61, 'Passenger ships, hazardous category A'),
            (62, 'Passenger ships, hazardous category B'),
            (63, 'Passenger ships, hazardous category C'),
            (64, 'Passenger ships, hazardous category D'),
            (65, 'Passenger ships'),
            (66, 'Passenger ships'),
            (67, 'Passenger ships'),
            (68, 'Passenger ships'),
            (69, 'Passenger ships'),
            (70, 'Cargo ships'),
            (71, 'Cargo ships, hazardous category A'),
            (72, 'Cargo ships, hazardous category B'),
            (73, 'Cargo ships, hazardous category C'),
            (74, 'Cargo ships, hazardous category D'),
            (75, 'Cargo ships'),
            (76, 'Cargo ships'),
            (77, 'Cargo ships'),
            (78, 'Cargo ships'),
            (79, 'Cargo ships'),
            (80, 'Tankers'),
            (81, 'Tankers, hazardous category A'),
            (82, 'Tankers, hazardous category B'),
            (83, 'Tankers, hazardous category C'),
            (84, 'Tankers, hazardous category D'),
            (85, 'Tankers'),
            (86, 'Tankers'),
            (87, 'Tankers'),
            (88, 'Tankers'),
            (89, 'Tankers'),
            (90, 'Other'),
            (91, 'Other, hazardous category A'),
            (92, 'Other, hazardous category B'),
            (93, 'Other, hazardous category C'),
            (94, 'Other, hazardous category D'),
            (95, 'Other'),
            (96, 'Other'),
            (97, 'Other'),
            (98, 'Other'),
            (99, 'Other'),
            (100, 'Unknown'),
        ))


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

    def __init__(self, dbpath):
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

        #self.conn.execute('PRAGMA synchronous=0')
        self.conn.execute('PRAGMA temp_store=MEMORY')
        self.conn.execute('PRAGMA threads=6')
        self.conn.commit()

        self.cur = self.conn.cursor()
        self.cur.execute('SELECT name FROM sqlite_master '
                         'WHERE type="table" AND name="coarsetype_ref";')
        if not self.cur.fetchall():
            create_table_coarsetype(self.cur)
        self.conn.commit()

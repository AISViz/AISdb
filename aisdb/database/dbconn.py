''' SQLite Database connection

    Also see: https://docs.python.org/3/library/sqlite3.html#connection-objects
'''

import os
from calendar import monthrange
from datetime import datetime
# import aiosqlite

from aisdb import sqlite3

_coarsetype_rows = [
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
    (59, 'Ships and aircraft of States not parties to an armed conflict'),
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
]

_create_coarsetype_table = '''
CREATE TABLE IF NOT EXISTS coarsetype_ref (
    coarse_type integer,
    coarse_type_txt character varying(75)
);'''

_create_coarsetype_index = ('CREATE UNIQUE INDEX IF NOT EXISTS '
                            'idx_coarsetype ON coarsetype_ref(coarse_type)')


def get_dbname(dbpath):
    name_ext = os.path.split(dbpath)[1]
    name = name_ext.split('.')[0]
    return name


pragmas = [
    'PRAGMA temp_store=MEMORY',
    'PRAGMA journal_mode=TRUNCATE',
    'PRAGMA threads=6',
    'PRAGMA mmap_size=1000000000',  # 1GB
    'PRAGMA cache_size=-15625000',  # 16GB
    'PRAGMA cache_spill=0',
]


class DBConn(sqlite3.Connection):
    ''' SQLite3 database connection object

        attributes:
            dbpaths (list of strings)
                currently attached databases. initialized as [],
                list becomes populated with databases using the .attach()
                method
            db_daterange (dict)
                temporal range of monthly database tables. keys are DB file
                names
    '''

    def _create_table_coarsetype(self):
        ''' create a table to describe integer vessel type as a human-readable
            string.
        '''
        self.execute(_create_coarsetype_table)
        self.execute(_create_coarsetype_index)
        self.executemany(('INSERT OR IGNORE INTO coarsetype_ref '
                          '(coarse_type, coarse_type_txt) VALUES (?,?) '),
                         _coarsetype_rows)
        self.commit()

    def __init__(self):
        # configs
        self.dbpaths = []
        self.db_daterange = {}
        super().__init__(':memory:',
                         timeout=5,
                         detect_types=sqlite3.PARSE_DECLTYPES
                         | sqlite3.PARSE_COLNAMES)
        self.row_factory = sqlite3.Row
        for p in pragmas:
            self.execute(p)
        self.commit()
        self._create_table_coarsetype()

    def __exit__(self, exc_class, exc, tb):
        for dbpath in self.dbpaths:
            self.execute('DETACH DATABASE ?', [get_dbname(dbpath)])
        self.commit()
        self.close()
        self = None

    def attach(self, dbpath):
        ''' connect to an additional database file '''
        assert get_dbname(dbpath) != 'main'
        if dbpath not in self.dbpaths:
            self.execute('ATTACH DATABASE ? AS ?',
                         [dbpath, get_dbname(dbpath)])
            self.dbpaths.append(dbpath)

        # query the temporal range of monthly database tables
        # results will be stored as a dictionary attribute db_daterange
        cur = self.cursor()
        sql_qry = (
            f'SELECT * FROM {get_dbname(dbpath)}.sqlite_master '
            'WHERE type="table" AND name LIKE "ais\\_%\\_dynamic" ESCAPE "\\" '
        )
        cur.execute(sql_qry)
        dynamic_tables = cur.fetchall()
        if dynamic_tables != []:
            db_months = sorted(
                [table['name'].split('_')[1] for table in dynamic_tables])
            self.db_daterange[get_dbname(dbpath)] = {
                'start':
                datetime(int(db_months[0][:4]), int(db_months[0][4:]),
                         1).date(),
                'end':
                datetime((y := int(db_months[-1][:4])),
                         (m := int(db_months[-1][4:])),
                         monthrange(y, m)[1]).date(),
            }

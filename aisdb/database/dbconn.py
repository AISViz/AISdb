''' SQLite Database connection

    Also see: https://docs.python.org/3/library/sqlite3.html#connection-objects
'''

from calendar import monthrange
from datetime import datetime
import os
import warnings

from aisdb import sqlite3, sqlpath


class DBConn(sqlite3.Connection):
    ''' SQLite3 database connection object

        attributes:
            dbpaths (list of strings)
                currently attached databases. initialized as [],
                list becomes populated with databases using the ._attach()
                method
            db_daterange (dict)
                temporal range of monthly database tables. keys are DB file
                names
    '''

    def _create_table_coarsetype(self):
        ''' create a table to describe integer vessel type as a human-readable
            string.
        '''
        with open(os.path.join(sqlpath, 'coarsetype.sql'), 'r') as f:
            coarsetype_sql = f.read().split(';')
        for row in coarsetype_sql:
            self.execute(row)
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
        '''
        cur = self.cursor()
        cur.execute("PRAGMA journal_mode")
        res = cur.fetchone()[0]
        if res != 'wal':
            self.execute('PRAGMA journal_mode=wal')
            self.commit()
        '''
        self._create_table_coarsetype()

    def __enter__(self):
        return self

    def __exit__(self, exc_class, exc, tb):
        try:
            for dbpath in self.dbpaths:
                self.execute('DETACH DATABASE ?', [self._get_dbname(dbpath)])
            #self.commit()
        except Exception:
            print('rolling back...')
            self.rollback()
        finally:
            self.close()
        self = None

    def _get_dbname(self, dbpath):
        name_ext = os.path.split(dbpath)[1]
        name = name_ext.split('.')[0]
        return name

    def _attach(self, dbpath):
        ''' connect to an additional database file '''
        assert self._get_dbname(dbpath) != 'main'

        if dbpath not in self.dbpaths:
            self.execute('ATTACH DATABASE ? AS ?',
                         [dbpath, self._get_dbname(dbpath)])
            self.dbpaths.append(dbpath)

        # query the temporal range of monthly database tables
        # results will be stored as a dictionary attribute db_daterange
        cur = self.cursor()
        sql_qry = (
            f'SELECT * FROM {self._get_dbname(dbpath)}.sqlite_master '
            'WHERE type="table" AND name LIKE "ais\\_%\\_dynamic" ESCAPE "\\" '
        )
        try:
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

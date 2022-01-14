''' class to convert a dictionary of input parameters into SQL code, and generate queries '''

from collections import UserDict
from datetime import datetime

import numpy as np
from shapely.geometry import Polygon

from aisdb.common import dbpath
from database.sqlfcn import crawl
from database.dbconn import DBConn
from database.sqlfcn_callbacks import dt2monthstr, arr2polytxt, epoch2monthstr
from database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)


class DBQuery(UserDict):
    ''' A database abstraction allowing the creation of SQL code via arguments
        passed to __init__(). Args are stored as a dictionary (UserDict).

        Args:
            callback: (function)
                anonymous function yielding SQL code specifying "WHERE" clauses.
                common queries are included in aisdb.database.sqlfcn_callbacks,
                e.g.

                >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi

                this generates SQL code to apply filtering on columns (mmsi, time),
                and requires (start, end) as arguments in datetime format.

                >>> q = DBQuery(callback=in_timerange_validmmsi,
                ...             start=datetime(2022, 1, 1),
                ...             end=datetime(2022, 1, 7),
                ...             )

                Resulting SQL is then passed to the query function as an argument.
            **kwargs:
                more arguments that will be supplied to the query function
                and callback function


        Custom SQL queries are supported by modifying the fcn supplied to .run_qry()
        and .gen_qry(), or by supplying a custom callback function.
        Alternatively, the database can also be queried directly, see
        DBConn.py for more info

        complete example:

        >>> from datetime import datetime
        >>> from aisdb import dbpath, DBQuery
        >>> from aisdb.database.lamdas import in_timerange_validmmsi

        >>> q = DBQuery(callback=in_timerange_validmmsi,
        ...             start=datetime(2022, 1, 1),
        ...             end=datetime(2022, 1, 7),
        ...             )

        >>> q.check_idx()  # build index if necessary
        >>> print(f'iterating over rows returned from {dbpath}')
        >>> for rows in q.gen_qry():
        ...     print(rows)
    '''

    def __init__(self, **kwargs):

        self.data = kwargs

        if 'xy' in self.keys() and 'x' not in self.keys(
        ) and 'y' not in self.keys():
            self['x'] = self['xy'][::2]
            self['y'] = self['xy'][1::2]

        # if sum(map(lambda t: t in kwargs.keys(), ('start', 'end',))) == 2:
        if 'start' in self.data.keys() and 'end' in self.data.keys():
            if isinstance(kwargs['start'], datetime):
                self.data.update({'months': dt2monthstr(**kwargs)})
            elif isinstance(kwargs['start'], (float, int)):
                self.data.update({'months': epoch2monthstr(**kwargs)})
            else:
                assert False

        if 'x' in self.data.keys() and 'y' in self.data.keys():
            xy = (self['x'], self['y'])
            matching = [(list, np.ndarray, tuple) for _ in range(2)]

            if sum(map(isinstance, xy, matching)) == 2:
                assert len(self['x']) == len(
                    self['y']), 'coordinate arrays are not equivalent length'
                assert Polygon(zip(self.data['x'],
                                   self.data['y'])).is_valid, 'invalid polygon'
                self.data['poly'] = arr2polytxt(x=self.data['x'],
                                                y=self.data['y'])

            else:
                assert 'radius' in self.keys(), 'undefined radius'

    def check_idx(self, dbpath=dbpath):
        ''' ensure that all tables exist, and indexes are built, for the
            timespan covered by the DBQuery
        '''
        aisdatabase = DBConn(dbpath)
        cur = aisdatabase.cur
        for month in self.data['months']:
            cur.execute(
                'SELECT * FROM sqlite_master WHERE type="table" and name=?',
                [f'ais_{month}_static'])
            if len(cur.fetchall()) == 0:
                sqlite_createtable_staticreport(cur, month)

            cur.execute(
                'SELECT * FROM sqlite_master WHERE type="table" and name=?',
                [f'static_{month}_aggregate'])

            if len(cur.fetchall()) == 0:
                print(f'building static index for month {month}...')
                aggregate_static_msgs(dbpath, [month])

            cur.execute(
                'SELECT * FROM sqlite_master WHERE type="table" and name=?',
                [f'ais_{month}_dynamic'])
            if len(cur.fetchall()) == 0:
                sqlite_createtable_dynamicreport(cur, month)

            cur.execute(
                'SELECT * FROM sqlite_master WHERE type="index" and name=?',
                [f'idx_{month}_t_x_y'])

            if len(cur.fetchall()) == 0:
                print(f'building dynamic index for month {month}...')
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS idx_{month}_t_x_y '
                    f'ON ais_{month}_dynamic (time, longitude, latitude)')

        aisdatabase.conn.commit()
        aisdatabase.conn.close()

    def run_qry(self, fcn=crawl, dbpath=dbpath, printqry=True):
        ''' queries the database

            args:
                self: (UserDict)
                    dictionary containing kwargs
                fcn: (function)
                    callback function that will generate SQL code using
                    the args stored in self
                dbpath: (string)
                    defaults to the database path configured in ~/.config/ais.cfg
                printqry: (boolean)
                    Optionally silence the messages printing SQL code to be
                    executed

            returns:
                resulting rows in array format

            CAUTION: may use an excessive amount of memory for large queries.
            consider using gen_qry instead
        '''

        q = fcn(**self)
        if printqry:
            print(q)

        aisdatabase = DBConn(dbpath)

        assert self.data['start'] < self.data['end'], 'invalid time range'
        assert len(self.data['months']) >= 1, f'bad qry {self=}'
        self.check_idx()

        aisdatabase.cur.execute(q)
        res = aisdatabase.cur.fetchall()
        aisdatabase.conn.close()
        return np.array(res)

    def gen_qry(self, fcn=crawl, dbpath=dbpath):
        ''' queries the database using the supplied SQL function and dbpath.
            generator only stores one item at at time before yielding

            args:
                self (UserDict)
                    dictionary containing kwargs
                fcn (function)
                    callback function that will generate SQL code using
                    the args stored in self
                dbpath (string)
                    defaults to the database path configured in ~/.config/ais.cfg

            yields:
                numpy array of rows for each unique MMSI
                arrays are sorted by MMSI
                rows are sorted by time
        '''
        self.check_idx()
        qry = fcn(**self)

        # initialize db, run query
        print(qry)
        print('\nquerying the database...')
        aisdatabase = DBConn(dbpath)
        dt = datetime.now()
        aisdatabase.cur.execute(qry)
        delta = datetime.now() - dt
        print(f'query time: {delta.total_seconds():.2f}s')

        # get 100k rows at a time, yield sets of rows for each unique MMSI
        mmsi_rows = None
        res = aisdatabase.cur.fetchmany(10**5)
        while len(res) > 0:
            if mmsi_rows is None:
                mmsi_rows = np.array(res, dtype=object)
            else:
                mmsi_rows = np.vstack((mmsi_rows, np.array(res, dtype=object)))

            print(f'{mmsi_rows[0][0]}', end='\r')

            while len(mmsi_rows) > 1 and int(mmsi_rows[0][0]) != int(
                    mmsi_rows[-1][0]):
                if not isinstance(mmsi_rows[0][0], (float, int)):
                    print(f'error: MMSI not an integer! {mmsi_rows[0]}')
                    breakpoint()
                if not isinstance(mmsi_rows, np.ndarray):
                    print(f'not an array: {mmsi_rows}')
                    breakpoint()
                ummsi_idx = np.where(mmsi_rows[:, 0] != mmsi_rows[0, 0])[0][0]
                yield np.array(mmsi_rows[0:ummsi_idx], dtype=object)
                mmsi_rows = mmsi_rows[ummsi_idx:]

            res = aisdatabase.cur.fetchmany(10**5)

        yield np.array(mmsi_rows, dtype=object)
        aisdatabase.conn.close()
        print('\ndone')

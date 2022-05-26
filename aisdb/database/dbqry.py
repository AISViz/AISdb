''' class to convert a dictionary of input parameters into SQL code, and
    generate queries
'''

import sqlite3
import aiosqlite
from collections import UserDict
from datetime import datetime
from functools import reduce

import numpy as np
from shapely.geometry import Polygon

from aisdb.database import sqlfcn_callbacks
from aisdb.database.dbconn import DBConn
from aisdb.database import sqlfcn
from aisdb.database.sqlfcn_callbacks import dt2monthstr, arr2polytxt
from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)
from aisdb.webdata.marinetraffic import VesselInfo

from aisdb.gis import epoch_2_dt

_epoch2monthstr = lambda start, end, **_: dt2monthstr(epoch_2_dt(start),
                                                      epoch_2_dt(end))


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

                >>> start, end = datetime(2022, 1, 1), datetime(2022, 1, 7)
                >>> q = DBQuery(callback=in_timerange_validmmsi, start=start, end=end)

                Resulting SQL is then passed to the query function as an argument.

            **kwargs:
                more arguments that will be supplied to the query function
                and callback function


        Custom SQL queries are supported by modifying the fcn supplied to .gen_qry()
        and .async_qry(), or by supplying a custom callback function.
        Alternatively, the database can also be queried directly, see
        DBConn.py for more info

        complete example:

        >>> from datetime import datetime
        >>> from aisdb import DBQuery
        >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi

        >>> dbpath = '~/ais/ais.db'
        >>> start, end = datetime(2022, 1, 1), datetime(2022, 1, 7)
        >>> q = DBQuery(callback=in_timerange_validmmsi, start=start, end=end)

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

            if self.data['start'] >= self.data['end']:
                raise ValueError('Start must occur before end')
            elif isinstance(kwargs['start'], datetime):
                self.data.update({'months': dt2monthstr(**kwargs)})
            elif isinstance(kwargs['start'], (float, int)):
                self.data.update({'months': _epoch2monthstr(**kwargs)})
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

    def check_marinetraffic(self,
                            dbpath,
                            trafficDBpath,
                            boundary,
                            data_dir,
                            retry_404=False):
        ''' scrape metadata for vessels in domain from marinetraffic

            args:
                dbpath (string)
                    database file path
                trafficDBpath (string)
                    marinetraffic database path
                boundary (dict)
                    uses keys xmin, xmax, ymin, and ymax to denote the region
                    of vessels that should be checked.
                    if using aisdb.gis.Domain, the Domain.boundary attribute
                    can be supplied here
        '''
        aisdatabase = DBConn(dbpath)
        cur = aisdatabase.cur
        vinfo = VesselInfo(trafficDBpath)
        for month in self.data['months']:
            print(f'retrieving vessel info for {month}', end='', flush=True)

            # create any missing tables
            cur.execute(
                'SELECT * FROM sqlite_master WHERE type="table" and name=?',
                [f'ais_{month}_dynamic'])
            if len(cur.fetchall()) == 0:
                sqlite_createtable_dynamicreport(cur, month)

            # check unique mmsis
            sql = f'''
            SELECT DISTINCT(mmsi) FROM ais_{month}_dynamic AS d WHERE
            {sqlfcn_callbacks.in_validmmsi_bbox(alias='d', **boundary)}
            '''
            cur.execute(sql)
            print('.', end='', flush=True)  # first dot
            mmsis = cur.fetchall()

            # retrieve vessel metadata
            if len(mmsis) > 0:
                vinfo.vessel_info_callback(mmsis=np.array(mmsis),
                                           data_dir=data_dir,
                                           retry_404=retry_404,
                                           infotxt=f'{month} ')

        aisdatabase.conn.close()

    def gen_qry(self,
                dbpath,
                fcn=sqlfcn.crawl_dynamic,
                printqry=False,
                force_reaggregate_static=False):
        ''' queries the database using the supplied SQL function and dbpath.
            generator only stores one item at at time before yielding

            args:
                self (UserDict)
                    dictionary containing kwargs
                dbpath (string)
                    database location. defaults to the path configured
                    in ~/.config/ais.cfg
                fcn (function)
                    callback function that will generate SQL code using
                    the args stored in self

            yields:
                numpy array of rows for each unique MMSI
                arrays are sorted by MMSI
                rows are sorted by time
        '''
        qry = fcn(**self)

        # initialize db, run query
        if printqry:
            print(qry)
        aisdatabase = DBConn(dbpath)

        for month in self.data['months']:
            #cur.execute(
            #    'SELECT * FROM sqlite_master WHERE type="table" and name=?',
            #    [f'ais_{month}_static'])
            #if len(cur.fetchall()) == 0:
            #    sqlite_createtable_staticreport(cur, month)
            aisdatabase.cur.execute(
                'SELECT * FROM sqlite_master WHERE type="table" and name=?',
                [f'static_{month}_aggregate'])

            if len(aisdatabase.cur.fetchall()
                   ) == 0 or force_reaggregate_static:
                print(f'building static index for month {month}...',
                      flush=True)
                aggregate_static_msgs(dbpath, [month])
            #cur.execute(
            #    'SELECT * FROM sqlite_master WHERE type="table" and name=?',
            #    [f'ais_{month}_dynamic'])
            #if len(cur.fetchall()) == 0:
            #    sqlite_createtable_dynamicreport(cur, month)

        dt = datetime.now()
        aisdatabase.cur.execute(qry)
        delta = datetime.now() - dt
        if printqry:
            print(
                f'query time: {delta.total_seconds():.2f}s\nfetching rows...')

        # get 500k rows at a time, yield sets of rows for each unique MMSI
        mmsi_rows = []
        res = aisdatabase.cur.fetchmany(10**5)
        assert res != [], f'no rows for query: {qry}'
        while len(res) > 0:
            mmsi_rows += res
            ummsi_idx = np.where(
                np.array(mmsi_rows)[:-1, 0] != np.array(mmsi_rows)[1:,
                                                                   0])[0] + 1
            ummsi_idx = reduce(np.append, ([0], ummsi_idx, [len(mmsi_rows)]))
            for i in range(len(ummsi_idx) - 2):
                yield mmsi_rows[ummsi_idx[i]:ummsi_idx[i + 1]]
            if len(ummsi_idx) > 2:
                mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]
            res = aisdatabase.cur.fetchmany(10**5)
        yield mmsi_rows
        aisdatabase.conn.close()

    async def async_qry(self,
                        dbpath,
                        fcn=sqlfcn.crawl_dynamic,
                        force_reaggregate_static=False):
        aisdatabase = await aiosqlite.connect(dbpath)
        aisdatabase.row_factory = sqlite3.Row
        for month in self.data['months']:
            precursor = await aisdatabase.execute(
                'SELECT * FROM sqlite_master WHERE type="table" and name=?',
                [f'static_{month}_aggregate'])

            if len(await
                   precursor.fetchall()) == 0 or force_reaggregate_static:
                print(f'building static index for month {month}...',
                      flush=True)
                aggregate_static_msgs(dbpath, [month])
        cursor = await aisdatabase.execute(fcn(**self))
        mmsi_rows = []
        res = await cursor.fetchmany(10**5)
        while len(res) > 0:
            mmsi_rows += res
            ummsi_idx = np.where(
                np.array(mmsi_rows)[:-1, 0] != np.array(mmsi_rows)[1:,
                                                                   0])[0] + 1
            ummsi_idx = reduce(np.append, ([0], ummsi_idx, [len(mmsi_rows)]))
            for i in range(len(ummsi_idx) - 2):
                yield mmsi_rows[ummsi_idx[i]:ummsi_idx[i + 1]]
            if len(ummsi_idx) > 2:
                mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]
            res = await cursor.fetchmany(10**5)
        yield mmsi_rows
        await cursor.close()
        await aisdatabase.close()

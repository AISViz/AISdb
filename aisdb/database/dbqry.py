''' class to convert a dictionary of input parameters into SQL code, and
    generate queries
'''

from collections import UserDict
from datetime import datetime
from functools import reduce

import numpy as np

from aisdb.database import sqlfcn_callbacks
from aisdb.database.dbconn import DBConn, DBConn_async, get_dbname
from aisdb.database import sqlfcn
from aisdb.database.sqlfcn_callbacks import dt2monthstr
from aisdb.database.create_tables import (
    aggregate_static_msgs,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)
from aisdb.webdata.marinetraffic import VesselInfo


class DBQuery(UserDict):
    ''' A database abstraction allowing the creation of SQL code via arguments
        passed to __init__(). Args are stored as a dictionary (UserDict).

        Args:
            callback: (function)
                anonymous function yielding SQL code specifying "WHERE" clauses.
                common queries are included in :mod:`aisdb.database.sqlfcn_callbacks.py`,
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

    def __init__(self, *, db, **kwargs):

        if not isinstance(db, (DBConn, DBConn_async)):
            raise ValueError(
                'db argument must be a DBConn database connection.'
                f'\tfound: {db}')

        self.data = kwargs
        self.db = db
        self.create_qry_params()

    def create_qry_params(self):
        assert 'start' in self.data.keys() and 'end' in self.data.keys()
        if self.data['start'] >= self.data['end']:
            raise ValueError('Start must occur before end')
        assert isinstance(self.data['start'], datetime)
        self.data.update({'months': dt2monthstr(**self.data)})
        #elif isinstance(self.data['start'],
        #                (float, int)):  # pragma: no cover
        #    self.data.update({'months': _epoch2monthstr(**self.data)})

    def check_marinetraffic(
            self,
            #dbpath,
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
        cur = self.db.cur
        vinfo = VesselInfo(trafficDBpath)
        # TODO: determine which attached db to query

        for dbpath in self.db.dbpaths:
            print(f'retrieving vessel info for {dbpath}', end='', flush=True)
            for month in self.data['months']:
                dbname = get_dbname(dbpath)
                self.db.attach(dbpath)

                # check for missing tables
                cur.execute(
                    f'SELECT * FROM {dbname}.sqlite_master WHERE type="table" and name=?',
                    [f'ais_{month}_dynamic'])
                if len(cur.fetchall()) == 0:  # pragma: no cover
                    continue

                # check unique mmsis
                sql = f'''
                SELECT DISTINCT(mmsi) FROM {dbname}.ais_{month}_dynamic AS d WHERE
                {sqlfcn_callbacks.in_validmmsi_bbox(alias='d', **boundary)}
                '''
                cur.execute(sql)
                print('.', end='', flush=True)  # first dot
                mmsis = cur.fetchall()

                # retrieve vessel metadata
                if len(mmsis) > 0:  # pragma: no cover
                    # not covered; will be cached for testing
                    vinfo.vessel_info_callback(mmsis=np.array(mmsis),
                                               data_dir=data_dir,
                                               retry_404=retry_404,
                                               infotxt=f'{month} ')

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
        dbname = get_dbname(dbpath)
        if 'dbpath' not in self.data.keys():
            self.data['dbpath'] = dbpath

        # initialize db, run query
        self.db.attach(dbpath)
        qry = fcn(**self.data)

        if printqry:
            print(qry)

        for month in self.data['months']:
            # create static tables if necessary
            self.db.cur.execute(
                f'SELECT * FROM {dbname}.sqlite_master WHERE type="table" and name=?',
                [f'ais_{month}_static'])
            if len(self.db.cur.fetchall()) == 0:
                sqlite_createtable_staticreport(self.db, month, dbpath=dbpath)

            # create aggregate tables if necessary
            self.db.cur.execute(
                f'SELECT * FROM {dbname}.sqlite_master WHERE type="table" and name=?',
                [f'static_{month}_aggregate'])

            if len(self.db.cur.fetchall()) == 0 or force_reaggregate_static:
                print(f'building static index for month {month}...',
                      flush=True)
                aggregate_static_msgs(self.db, [month])

            # create dynamic tables if necessary
            self.db.cur.execute(
                f'SELECT * FROM {dbname}.sqlite_master WHERE type="table" and name=?',
                [f'ais_{month}_dynamic'])
            if len(self.db.cur.fetchall()) == 0:  # pragma: no cover
                sqlite_createtable_dynamicreport(self.db, month, dbpath=dbpath)

        dt = datetime.now()
        self.db.cur.execute(qry)
        delta = datetime.now() - dt
        if printqry:
            print(
                f'query time: {delta.total_seconds():.2f}s\nfetching rows...')

        # get 500k rows at a time, yield sets of rows for each unique MMSI
        mmsi_rows = []
        res = self.db.cur.fetchmany(10**5)

        if res == []:  # pragma: no cover
            raise SyntaxError(f'no results for query!\n{qry}')

        while len(res) > 0:
            mmsi_rows += res
            ummsi_idx = np.where(
                np.array(mmsi_rows)[:-1, 0] != np.array(mmsi_rows)[1:,
                                                                   0])[0] + 1
            ummsi_idx = reduce(np.append, ([0], ummsi_idx, [len(mmsi_rows)]))
            for i in range(len(ummsi_idx) - 2):
                yield mmsi_rows[ummsi_idx[i]:ummsi_idx[i + 1]]
            #if len(ummsi_idx) > 2:
            #    mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]
            assert len(ummsi_idx) > 2
            mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]

            res = self.db.cur.fetchmany(10**5)
        yield mmsi_rows


class DBQuery_async(DBQuery):

    def __init__(self, *, db, **kwargs):

        self.data = kwargs
        self.db = db
        self.create_qry_params()

    async def gen_qry(self,
                      dbpath,
                      fcn=sqlfcn.crawl_dynamic,
                      force_reaggregate_static=False):

        for month in self.data['months']:
            res = await self.db.conn.execute_fetchall(
                ('SELECT * FROM main.sqlite_master '
                 'WHERE type="table" and name=?'),
                [f'static_{month}_aggregate'])
            if res == []:  # pragma: no cover
                with DBConn() as syncdb:
                    print('Aggregating static messages synchronously... '
                          'This should only happen once!')
                    aggregate_static_msgs(syncdb, [month])

        self.data['dbpath'] = 'main'
        qry = fcn(**self)
        cursor = await self.db.conn.execute(qry)
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
            #if len(ummsi_idx) > 2:
            #    mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]
            assert len(ummsi_idx) > 2
            mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]

            res = await cursor.fetchmany(10**5)
        yield mmsi_rows
        await cursor.close()

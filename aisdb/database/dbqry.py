''' class to convert a dictionary of input parameters into SQL code, and
    generate queries
'''

from collections import UserDict
from datetime import datetime
from functools import reduce
import sqlite3

import numpy as np
import aiosqlite

from aisdb.database import sqlfcn_callbacks
from aisdb.database.dbconn import (
    DBConn,
    _coarsetype_rows,
    _create_coarsetype_index,
    _create_coarsetype_table,
    get_dbname,
    pragmas,
)
from aisdb.database import sqlfcn
from aisdb.database.sqlfcn_callbacks import dt2monthstr
from aisdb.database.create_tables import (
    aggregate_static_msgs, )
from aisdb.webdata.marinetraffic import VesselInfo


class DBQuery(UserDict):
    ''' A database abstraction allowing the creation of SQL code via arguments
        passed to __init__(). Args are stored as a dictionary (UserDict).

        Args:
            dbconn (:class:`aisdb.database.dbconn.DBConn`)
                database connection object
            dbpath (string)
                database filepath to connect to
            dbpaths (list)
                optionally pass a list of filepaths instead of a single dbpath
            callback (function)
                anonymous function yielding SQL code specifying "WHERE" clauses.
                common queries are included in :mod:`aisdb.database.sqlfcn_callbacks.py`,
                e.g.

                >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi

                this generates SQL code to apply filtering on columns (mmsi, time),
                and requires (start, end) as arguments in datetime format.

                >>> start, end = datetime(2022, 1, 1), datetime(2022, 1, 7)
                >>> q = DBQuery(callback=in_timerange_validmmsi, start=start, end=end)

                Resulting SQL is then passed to the query function as an argument.

            **kwargs (dict)
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
        >>> with DBConn() as dbconn:
        >>>     q = DBQuery(dbconn=dbconn,
        >>>                 dbpath=dbpath,
        >>>                 callback=in_timerange_validmmsi,
        >>>                 start=start,
        >>>                 end=end)
        >>>     for rows in q.gen_qry():
        ...         print(rows)
    '''

    def __init__(self, *, dbconn, dbpath=None, dbpaths=[], **kwargs):
        if dbpaths == [] and dbpath is None:
            raise ValueError(
                'must supply either dbpaths list or dbpath string value')
        elif dbpaths == []:  # pragma: no cover
            dbpaths = [dbpath]

        for dbpath in dbpaths:
            dbconn.attach(dbpath)
        if not isinstance(dbconn, DBConn):
            raise ValueError(
                'db argument must be a DBConn database connection.'
                f'\tfound: {dbconn}')

        self.data = kwargs
        self.dbconn = dbconn
        self.create_qry_params()

    def create_qry_params(self):
        assert 'start' in self.data.keys() and 'end' in self.data.keys()
        if self.data['start'] >= self.data['end']:
            raise ValueError('Start must occur before end')
        assert isinstance(self.data['start'], datetime)
        self.data.update({'months': dt2monthstr(**self.data)})

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
        self.dbconn.attach(dbpath)
        vinfo = VesselInfo(trafficDBpath)
        # TODO: determine which attached db to query

        print(f'retrieving vessel info for {dbpath}', end='', flush=True)
        for month in self.data['months']:
            dbname = get_dbname(dbpath)
            self.dbconn.attach(dbpath)

            # skip missing tables
            if self.dbconn.execute(
                (f'SELECT * FROM {dbname}.sqlite_master '
                 'WHERE type="table" and name=?'),
                [f'ais_{month}_dynamic']).fetchall() == 0:  # pragma: no cover
                continue

            # check unique mmsis
            sql = (
                'SELECT DISTINCT(mmsi) '
                f'FROM {dbname}.ais_{month}_dynamic AS d WHERE '
                f'{sqlfcn_callbacks.in_validmmsi_bbox(alias="d", **boundary)}')
            mmsis = self.dbconn.execute(sql).fetchall()
            print('.', end='', flush=True)  # first dot

            # retrieve vessel metadata
            if len(mmsis) > 0:  # pragma: no cover
                # not covered due to caching used for testing
                vinfo.vessel_info_callback(mmsis=np.array(mmsis),
                                           retry_404=retry_404,
                                           infotxt=f'{month} ')

    def gen_qry(self,
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

        # initialize dbconn, run query
        assert 'dbpath' not in self.data.keys()

        for dbpath in self.dbconn.dbpaths:
            qry = fcn(dbpath=dbpath, **self.data)
            if printqry:
                print(qry)

            # get 500k rows at a time, yield sets of rows for each unique MMSI
            mmsi_rows = []
            dt = datetime.now()
            cur = self.dbconn.cursor()
            _ = cur.execute(qry)
            res = cur.fetchmany(10**5)
            delta = datetime.now() - dt
            if printqry:
                print(
                    f'query time: {delta.total_seconds():.2f}s\nfetching rows...'
                )
            if res == []:
                raise SyntaxError(f'no results for query!\n{qry}')

            while len(res) > 0:
                mmsi_rows += res
                ummsi_idx = np.where(
                    np.array(mmsi_rows)[:-1, 0] != np.array(mmsi_rows)[1:, 0]
                )[0] + 1
                ummsi_idx = reduce(np.append,
                                   ([0], ummsi_idx, [len(mmsi_rows)]))
                for i in range(len(ummsi_idx) - 2):
                    yield mmsi_rows[ummsi_idx[i]:ummsi_idx[i + 1]]
                if len(ummsi_idx) > 2:
                    mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]

                res = cur.fetchmany(10**5)
            yield mmsi_rows


class DBQuery_async(DBQuery):

    async def create_table_coarsetype(self):
        ''' create a table to describe integer vessel type as a human-readable string
            included here instead of create_tables.py to prevent circular import error
        '''

        _ = await self.dbconn.execute(_create_coarsetype_table)

        _ = await self.dbconn.execute(_create_coarsetype_index)

        _ = await self.dbconn.executemany((
            'INSERT OR IGNORE INTO coarsetype_ref (coarse_type, coarse_type_txt) '
            'VALUES (?,?) '), _coarsetype_rows)

    def __init__(self, *, dbpath, **kwargs):

        self.data = kwargs
        self.dbpath = dbpath
        self.create_qry_params()

    async def gen_qry(self,
                      fcn=sqlfcn.crawl_dynamic,
                      force_reaggregate_static=False):

        if not hasattr(self, 'dbconn'):
            self.dbconn = await aiosqlite.connect(self.dbpath)
            self.dbconn.row_factory = sqlite3.Row

        for p in pragmas:
            _ = await self.dbconn.execute(p)

        _ = await self.create_table_coarsetype()

        for month in self.data['months']:
            res = await self.dbconn.execute_fetchall(
                ('SELECT * FROM main.sqlite_master '
                 'WHERE type="table" and name=?'),
                [f'static_{month}_aggregate'])
            if res == []:  # pragma: no cover
                with DBConn() as syncdb:
                    print('Aggregating static messages synchronously... ')
                    aggregate_static_msgs(syncdb, [month])

        qry = fcn(dbpath='main', **self)
        cursor = await self.dbconn.execute(qry)
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
            assert len(ummsi_idx) > 2
            mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]

            res = await cursor.fetchmany(10**5)
        yield mmsi_rows
        await cursor.close()
        await self.dbconn.close()

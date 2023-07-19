''' class to convert a dictionary of input parameters into SQL code, and
    generate queries
'''

from collections import UserDict
from datetime import datetime, timedelta, date
from functools import reduce
import warnings

import numpy as np

from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.create_tables import (aggregate_static_msgs,
                                          sqlite_createtable_dynamicreport,
                                          sqlite_createtable_staticreport)
from aisdb.database.dbconn import ConnectionType, PostgresDBConn, SQLiteDBConn
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
                anonymous function yielding SQL code specifying "WHERE"
                clauses. common queries are included in
                :mod:`aisdb.database.sqlfcn_callbacks`, e.g.
                >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi
                >>> callback = in_timerange_validmmsi

                this generates SQL code to apply filtering on columns (mmsi,
                time), and requires (start, end) as arguments in datetime
                format.

            **kwargs (dict)
                more arguments that will be supplied to the query function
                and callback function


        Custom SQL queries are supported by modifying the fcn supplied to
        .gen_qry() and .async_qry(), or by supplying a callback function.
        Alternatively, the database can also be queried directly, see
        DBConn.py for more info

        complete example:

        >>> import os
        >>> from datetime import datetime
        >>> from aisdb import DBConn, DBQuery, decode_msgs
        >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi

        >>> dbpath = './testdata/test.db'
        >>> start, end = datetime(2021, 7, 1), datetime(2021, 7, 7)
        >>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv',
        ...              'aisdb/tests/testdata/test_data_20211101.nm4']
        >>> with DBConn() as dbconn:
        ...     decode_msgs(filepaths=filepaths, dbconn=dbconn, dbpath=dbpath,
        ...     source='TESTING')
        ...     q = DBQuery(dbconn=dbconn,
        ...                 dbpath=dbpath,
        ...                 callback=in_timerange_validmmsi,
        ...                 start=start,
        ...                 end=end)
        ...     for rows in q.gen_qry():
        ...         print(str(dict(rows[0])))
        ...         break
        {'mmsi': 204242000, 'time': 1625176725, 'longitude': -8.93166666667, 'latitude': 41.45, 'sog': 4.0, 'cog': 176.0}
    '''

    def __init__(self, *, dbconn, dbpath=None, dbpaths=[], **kwargs):
        #uif isinstance(dbconn, ConnectionType.SQLITE.value):

        if isinstance(dbconn, SQLiteDBConn):
            if dbpaths == [] and dbpath is None:
                raise ValueError(
                    'must supply either dbpaths list or dbpath string value')
            elif dbpaths == []:  # pragma: no cover
                dbpaths = [dbpath]
            # elif isinstance(dbconn, ConnectionType.POSTGRES):

        elif isinstance(dbconn, PostgresDBConn):
            if dbpath is not None:
                raise ValueError(
                    "the dbpath argument may not be used with a Postgres connection"
                )
        else:
            raise ValueError("Invalid database connection")

        for dbpath in dbpaths:
            dbconn._attach(dbpath)
        if isinstance(dbconn, ConnectionType):
            raise ValueError('Invalid database connection.'
                             f' Got: {dbconn}.'
                             f'Requires: {ConnectionType.SQLITE.value}'
                             f' or {ConnectionType.POSTGRES.value}')

        self.data = kwargs
        self.dbconn = dbconn
        self.create_qry_params()

    def create_qry_params(self):
        assert 'start' in self.data.keys() and 'end' in self.data.keys()
        if self.data['start'] >= self.data['end']:
            raise ValueError('Start must occur before end')
        assert isinstance(self.data['start'], (datetime, date))
        self.data.update({'months': sqlfcn_callbacks.dt2monthstr(**self.data)})

    def check_marinetraffic(self,
                            dbpath,
                            trafficDBpath,
                            boundary,
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
                    if using :class:`aisdb.gis.Domain`, the `Domain.boundary`
                    attribute can be supplied here
        '''
        self.dbconn._attach(dbpath)
        vinfo = VesselInfo(trafficDBpath)
        # TODO: determine which attached db to query

        print(f'retrieving vessel info for {dbpath}', end='', flush=True)
        for month in self.data['months']:
            dbname = self.dbconn._get_dbname(dbpath)
            self.dbconn._attach(dbpath)

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
                reaggregate_static=False,
                verbose=False):
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
                verbose (bool)
                    log info to stdout

            yields:
                numpy array of rows for each unique MMSI
                arrays are sorted by MMSI
                rows are sorted by time
        '''

        # initialize dbconn, run query
        assert 'dbpath' not in self.data.keys()
        cur = self.dbconn.cursor()

        if isinstance(self.dbconn, PostgresDBConn):
            iter_names = ['main']
        elif isinstance(self.dbconn, SQLiteDBConn):
            iter_names = [
                f for f in self.dbconn.dbpaths
                if self.dbconn._get_dbname(f) in self.dbconn.db_daterange
            ]
        else:
            assert False

        #for dbpath in self.dbconn.dbpaths:
        for dbpath in iter_names:
            #if self.dbconn._get_dbname(dbpath) not in self.dbconn.db_daterange:
            #    continue

            db_rng = self.dbconn.db_daterange[self.dbconn._get_dbname(dbpath)]

            if self['start'].date() > db_rng['end'] or self['end'].date(
            ) < db_rng['start']:
                if verbose:
                    print(f'skipping query for {dbpath} (out of timerange)...')
                continue

            for month in self.data['months']:

                month_date = datetime(int(month[:4]), int(month[4:]), 1)
                qry_start = self["start"] - timedelta(days=self["start"].day)
                assert qry_start <= month_date <= self[
                    'end'], f'{month_date} not in range ({qry_start}->{self["end"]})'

                rng_string = f'{db_rng["start"].year}-{db_rng["start"].month:02d}-{db_rng["start"].day:02d}'
                rng_string += ' -> '
                rng_string += f'{db_rng["end"].year}-{db_rng["end"].month:02d}-{db_rng["end"].day:02d}'

                # check if static tables exist
                cur.execute(
                    f'SELECT * FROM {self.dbconn._get_dbname(dbpath)}.sqlite_master '
                    'WHERE type="table" AND name=?', [f'ais_{month}_static'])
                if len(cur.fetchall()) == 0:
                    #sqlite_createtable_staticreport(self.dbconn, month, dbpath)
                    warnings.warn('No static data for selected time range! '
                                  f'{self.dbconn._get_dbname(dbpath)} '
                                  f'{rng_string}')

                # check if aggregate tables exist
                cur.execute((
                    f'SELECT * FROM {self.dbconn._get_dbname(dbpath)}.sqlite_master '
                    'WHERE type="table" and name=?'),
                            [f'static_{month}_aggregate'])
                res = cur.fetchall()

                if len(res) == 0 or reaggregate_static:
                    if verbose:
                        print(f'building static index for month {month}...',
                              flush=True)
                    aggregate_static_msgs(self.dbconn, [month], verbose)

                # check if dynamic tables exist
                cur.execute(
                    f'SELECT * FROM {self.dbconn._get_dbname(dbpath)}.sqlite_master WHERE '
                    'type="table" and name=?', [f'ais_{month}_dynamic'])
                if len(cur.fetchall()) == 0:  # pragma: no cover
                    if isinstance(self.dbconn, ConnectionType.SQLITE.value):
                        sqlite_createtable_dynamicreport(
                            self.dbconn, month, dbpath)

                    warnings.warn('No data for selected time range! '
                                  f'{self.dbconn._get_dbname(dbpath)} '
                                  f'{rng_string}')

            qry = fcn(dbpath=dbpath, **self.data)
            if verbose:
                print(qry)

            # get 500k rows at a time, yield sets of rows for each unique MMSI
            mmsi_rows = []
            dt = datetime.now()
            #cur = self.dbconn.cursor()
            _ = cur.execute(qry)
            res = cur.fetchmany(10**5)
            delta = datetime.now() - dt
            if verbose:
                print(
                    f'query time: {delta.total_seconds():.2f}s\nfetching rows...'
                )
            if res == []:
                # raise SyntaxError(f'no results for query!\n{qry}')
                warnings.warn('No results for query!')

            while len(res) > 0:
                mmsi_rows += res
                ummsi_idx = np.where(
                    np.array(mmsi_rows)[:-1,
                                        0] != np.array(mmsi_rows)[1:,
                                                                  0])[0] + 1
                ummsi_idx = reduce(np.append,
                                   ([0], ummsi_idx, [len(mmsi_rows)]))
                for i in range(len(ummsi_idx) - 2):
                    yield mmsi_rows[ummsi_idx[i]:ummsi_idx[i + 1]]
                if len(ummsi_idx) > 2:
                    mmsi_rows = mmsi_rows[ummsi_idx[i + 1]:]

                res = cur.fetchmany(10**5)
            yield mmsi_rows

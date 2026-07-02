"""class to convert a dictionary of input parameters into SQL code, and
generate queries
"""

import os
import sqlite3
import warnings
from collections import UserDict
from datetime import date, datetime, timedelta
from functools import reduce

import numpy as np
import psycopg

from aisdb.database import sqlfcn, sqlfcn_callbacks
from aisdb.database.create_tables import sql_createtable_dynamic
from aisdb.database.dbconn import PostgresDBConn, SQLiteDBConn


class DBQuery(UserDict):
    """A database abstraction allowing the creation of SQL code via arguments
    passed to __init__(). Args are stored as a dictionary (UserDict).

    Args:
        dbconn (:class:`aisdb.database.dbconn.ConnectionType`)
            database connection object
        dbpath (string)
            optional database filepath. must match the filepath of the
            supplied SQLite connection, and may not be used with a
            Postgres connection
        callback (function)
            anonymous function yielding SQL code specifying "WHERE"
            clauses. common queries are included in
            :mod:`aisdb.database.sqlfcn_callbacks`, e.g.
            >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi
            >>> callback = in_timerange_validmmsi

            this generates SQL code to apply filtering on columns (mmsi,
            time), and requires (start, end) as arguments in datetime
            format.
        limit (int)
            Optionally limit the database query to a finite number of rows

        **kwargs (dict)
            more arguments that will be supplied to the query function
            and callback function


    Custom SQL queries are supported by modifying the fcn supplied to
    .gen_qry(), or by supplying a callback function.
    Alternatively, the database can also be queried directly, see
    dbconn.py for more info

    complete example:

    >>> import os
    >>> from datetime import datetime
    >>> from aisdb import SQLiteDBConn, DBQuery, decode_msgs
    >>> from aisdb.database.sqlfcn_callbacks import in_timerange_validmmsi

    >>> dbpath = './testdata/test.db'
    >>> start, end = datetime(2021, 7, 1), datetime(2021, 7, 7)
    >>> filepaths = ['aisdb/tests/testdata/test_data_20210701.csv', 'aisdb/tests/testdata/test_data_20211101.nm4']
    >>> with SQLiteDBConn(dbpath) as dbconn:
    ...     decode_msgs(filepaths=filepaths, dbconn=dbconn, source='TESTING', verbose=False)
    ...     q = DBQuery(dbconn=dbconn, callback=in_timerange_validmmsi, start=start, end=end)
    ...     for rows in q.gen_qry():
    ...         assert dict(rows[0]) == {'mmsi': 204242000, 'time': 1625176725,
    ...                                  'longitude': -8.93166666667, 'latitude': 41.45,
    ...                                  'sog': 4.0, 'cog': 176.0}
    ...         break
    """

    def __init__(self, *, dbconn, dbpath=None, **kwargs):
        if not isinstance(dbconn, (SQLiteDBConn, PostgresDBConn)):
            raise ValueError(f"Invalid database connection: {dbconn}")

        if dbpath is not None:
            if isinstance(dbconn, PostgresDBConn):
                raise ValueError(
                    "the dbpath argument may not be used with a Postgres connection"
                )
            if os.path.abspath(dbpath) != os.path.abspath(dbconn.dbpath):
                raise ValueError(
                    f"dbpath {dbpath!r} does not match the connected database "
                    f"{dbconn.dbpath!r}"
                )

        self.data = kwargs
        self.dbconn = dbconn
        self.create_qry_params()

    def create_qry_params(self):
        assert "start" in self.data.keys() and "end" in self.data.keys()
        if self.data["start"] >= self.data["end"]:
            raise ValueError("Start must occur before end")
        assert isinstance(self.data["start"], (datetime, date))
        self.data.update({"months": sqlfcn_callbacks.dt2monthstr(**self.data)})

    def _build_tables_sqlite(
        self,
        cur: sqlite3.Cursor,
        month: str,
        rng_string: str,
        reaggregate_static: bool = False,
        verbose: bool = False,
    ):
        # check if static tables exist
        cur.execute(
            'SELECT * FROM sqlite_master WHERE type="table" AND name=?',
            [f"ais_{month}_static"],
        )
        if len(cur.fetchall()) == 0:
            warnings.warn(f"No results found in ais_{month}_static")

        # check if aggregate tables exist
        cur.execute(
            ('SELECT * FROM sqlite_master WHERE type="table" and name=?'),
            [f"static_{month}_aggregate"],
        )
        res = cur.fetchall()

        if len(res) == 0 or reaggregate_static:
            if verbose:
                print(f"building static index for month {month}...", flush=True)
            self.dbconn.aggregate_static_msgs([month], verbose)

        # check if dynamic tables exist
        cur.execute(
            'SELECT * FROM sqlite_master WHERE type="table" and name=?',
            [f"ais_{month}_dynamic"],
        )
        if len(cur.fetchall()) == 0:
            if isinstance(self.dbconn, SQLiteDBConn):
                self.dbconn.execute(sql_createtable_dynamic.format(month))

            warnings.warn(f"No data for selected time range! {rng_string}")

    def _build_tables_postgres(
        self,
        cur: psycopg.Cursor,
        month: str,
        rng_string: str,
        reaggregate_static: bool = False,
        verbose: bool = False,
    ):

        # check if static tables exist
        static_qry = psycopg.sql.SQL("""
            SELECT table_name
            FROM information_schema.tables
            WHERE information_schema.tables.table_name = {TABLE}
        """).format(TABLE=psycopg.sql.Literal(f"ais_{month}_static"))
        cur.execute(static_qry)
        count_static = cur.fetchall()

        if len(count_static) == 0:
            warnings.warn(f"No static data for selected time range! {rng_string}")

        # check if aggregate tables exist
        cur.execute(
            psycopg.sql.SQL("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = {TABLE}
        """).format(TABLE=psycopg.sql.Literal(f"static_{month}_aggregate"))
        )
        res = cur.fetchall()

        if len(res) == 0 or reaggregate_static:
            if verbose:
                print(f"building static index for month {month}...", flush=True)
            self.dbconn.aggregate_static_msgs([month], verbose)

        # check if dynamic tables exist
        cur.execute(
            psycopg.sql.SQL("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = {TABLE}
        """).format(TABLE=psycopg.sql.Literal(f"ais_{month}_dynamic"))
        )

        if len(cur.fetchall()) == 0:  # pragma: no cover
            warnings.warn(f"No data for selected time range! {rng_string}")

    def gen_qry(
        self, fcn=sqlfcn.crawl_dynamic, reaggregate_static=False, verbose=False
    ):
        """queries the database using the supplied SQL function.

        args:
            self (UserDict)
                Dictionary containing keyword arguments
            fcn (function)
                Callback function that will generate SQL code using
                the args stored in self
            reaggregate_static (bool)
                If True, the metadata aggregate tables will be regenerated
                from
            verbose (bool)
                Log info to stdout

        yields:
            numpy array of rows for each unique MMSI
            arrays are sorted by MMSI
            rows are sorted by time
        """

        # initialize dbconn, run query
        assert "dbpath" not in self.data.keys()
        db_rng = self.dbconn.db_daterange

        if not self.dbconn.db_daterange:
            if verbose:
                print("skipping query (empty database)...")
            return
        elif self["start"].date() > db_rng["end"]:
            if verbose:
                print("skipping query (out of timerange)...")
            return
        elif self["end"].date() < db_rng["start"]:
            if verbose:
                print("skipping query (out of timerange)...")
            return

        assert isinstance(db_rng["start"], date)
        assert isinstance(db_rng["end"], date)

        cur = self.dbconn.cursor()
        for month in self.data["months"]:
            month_date = datetime(int(month[:4]), int(month[4:]), 1)
            qry_start = self["start"] - timedelta(days=self["start"].day)

            if not (qry_start <= month_date <= self["end"]):
                raise ValueError(
                    f"{month_date} not in data range ({qry_start}->{self['end']})"
                )

            rng_string = f"{db_rng['start'].year}-{db_rng['start'].month:02d}-{db_rng['start'].day:02d}"
            rng_string += " -> "
            rng_string += f"{db_rng['end'].year}-{db_rng['end'].month:02d}-{db_rng['end'].day:02d}"

            if isinstance(self.dbconn, SQLiteDBConn):
                self._build_tables_sqlite(
                    cur, month, rng_string, reaggregate_static, verbose
                )
            elif isinstance(self.dbconn, PostgresDBConn):
                self._build_tables_postgres(
                    cur, month, rng_string, reaggregate_static, verbose
                )
            else:
                assert False

        qry = fcn(**self.data)

        if "limit" in self.data.keys():
            # int() coercion closes the injection vector for the LIMIT value
            qry += f"\nLIMIT {int(self.data['limit'])}"

        if verbose:
            print(qry)

        # get 500k rows at a time, yield sets of rows for each unique MMSI
        mmsi_rows: list = []
        dt = datetime.now()
        _ = cur.execute(qry)
        res: list = cur.fetchmany(10**5)
        delta = datetime.now() - dt

        if verbose:
            print(f"query time: {delta.total_seconds():.2f}s\nfetching rows...")
        if res == []:
            warnings.warn("No results for query!")

        while len(res) > 0:
            mmsi_rows += res
            mmsi_rowvals = np.array([r["mmsi"] for r in mmsi_rows])
            ummsi_idx = np.where(mmsi_rowvals[:-1] != mmsi_rowvals[1:])[0] + 1
            ummsi_idx = reduce(np.append, ([0], ummsi_idx, [len(mmsi_rows)]))
            for i in range(len(ummsi_idx) - 2):
                yield mmsi_rows[ummsi_idx[i] : ummsi_idx[i + 1]]
            if len(ummsi_idx) > 2:
                mmsi_rows = mmsi_rows[ummsi_idx[i + 1] :]

            res = cur.fetchmany(10**5)
        yield mmsi_rows

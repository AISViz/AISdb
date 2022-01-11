''' class to convert a dictionary of input parameters into SQL code, and generate queries '''

import os
from collections import UserDict
from datetime import datetime

import numpy as np
from shapely.geometry import Polygon

from aisdb.common import dbpath, data_dir
from database.qryfcn import crawl
from database.dbconn import DBConn
from database.lambdas import dt2monthstr, arr2polytxt, epoch2monthstr
from database.create_tables import (
    aggregate_static_msgs,
    createfcns,
    sqlite_createtable_dynamicreport,
    sqlite_createtable_staticreport,
)


class DBQuery(UserDict):

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

    def run_qry(self, fcn=crawl, dbpath=dbpath, printqry=True):
        ''' queries the database using the supplied sql function and dbpath

            self: UserDict
                dictionary containing kwargs

            returns resulting rows

            CAUTION: may use an excessive amount of memory for large queries
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
        ''' similar to run_qry, but in a generator format
            only stores one item in memory at a time

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

        print('\ndone')

    def gen_dbfile(self,
                   newdb=os.path.join(data_dir, 'export.db'),
                   dbpath=dbpath):
        ''' export rows matching the callback into a new sqlite database file '''

        assert 'callback' in self.data.keys()
        exportdb = DBConn(newdb)
        aisdatabase = DBConn(dbpath)

        for mstr in self['months']:  #exportdb.cur.execute()
            for msg, fcn in createfcns.items():
                exportdb.cur.execute(
                    'SELECT * FROM sqlite_master WHERE type="table" AND name LIKE ?',
                    [f'%{mstr}%msg_' + msg.split('msg')[1] + '%'])
                if not exportdb.cur.fetchall():
                    fcn(exportdb.cur, mstr)

            for tablename, alias in zip([f'rtree_{mstr}_msg_1_2_3'],
                                        ['m123', 'm18']):
                aisdatabase.cur.execute(
                    f'SELECT * FROM ? WHERE {self["callback"](month=mstr,alias=alias)}',
                    [tablename])
                res = aisdatabase.cur.fetchmany(10**5)
                while len(res) > 0:
                    exportdb.cur.executemany(
                        f'INSERT {",".join(["?" for _ in range(len(res[0]))])} INTO {tablename}',
                        res)
                    res = aisdatabase.cur.fetchmany(10**5)
            exportdb.conn.commit()

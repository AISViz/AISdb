import os
from collections import UserDict
from functools import partial
from datetime import datetime
#import threading
import concurrent.futures

import numpy as np
from shapely.geometry import Polygon

from common import dbpath
from database.qryfcn import crawl
from database.dbconn import dbconn
from database.lambdas import dt2monthstr


class qrygen(UserDict):

    def __init__(self, **kwargs):

        self.data = kwargs

        if 'xy' in self.keys() and not 'x' in self.keys() and not 'y' in self.keys(): 
            self['x'] = self['xy'][::2]
            self['y'] = self['xy'][1::2]

        #if sum(map(lambda t: t in kwargs.keys(), ('start', 'end',))) == 2: 
        if 'start' in self.data.keys() and 'end' in self.data.keys(): 
            if isinstance(kwargs['start'], datetime):
                self.data.update({'months':dt2monthstr(**kwargs)})
            elif isinstance(kwargs['start'], (float, int)):
                self.data.update({'months':epoch2monthstr(**kwargs)})
            else: assert False

        if 'x' in self.data.keys() and 'y' in self.data.keys():

            if sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2: 
                assert len(self['x']) == len(self['y']),                     'coordinate arrays are not equivalent length'
                assert Polygon(zip(self.data['x'],self.data['y'])).is_valid, 'invalid polygon'

                self.data['poly'] = arr2polytxt(x=self.data['x'], y=self.data['y'])

            else:
                assert 'radius' in self.keys(), 'undefined radius'


    #def crawl(self):
    #    ''' returns an SQL query to crawl the database 
    #        query generated using given query function, parameters stored in self, and a callback function 
    #    '''
    #    #return '\nUNION '.join(map(partial(qryfcn, callback=callback, kwargs=self), self['months'])) + '\nORDER BY 1, 2'
    #    return crawl(**self)


    def run_qry(self, fcn=crawl, dbpath=dbpath):
        ''' generates an query using self.crawl(), runs it, then returns the resulting rows '''
        #qry = self.crawl(callback=callback, qryfcn=qryfcn)
        qry = fcn(**self)
        print(qry)

        aisdb = dbconn(dbpath)
        aisdb.cur.execute(qry)
        res = aisdb.cur.fetchall()
        aisdb.conn.close()
        return np.array(res) 
        '''
        with concurrent.futures.ThreadPoolExecutor() as executor:
            #aisdb = dbconn(dbpath)
            future = executor.submit(self.qry_thread, dbpath=dbpath, qry=qry)
            try:
                res = future.result()
            except KeyboardInterrupt as err:
                print('interrupted!')
                aisdb.conn.interrupt()
            except Exception as err:
                raise err
            finally:
                aisdb.conn.close()
        '''


    def gen_qry(self, fcn=crawl, dbpath=dbpath):
        ''' similar to run_qry, but in a generator format for better memory performance
            
            yields:
                a set (numpy array) of rows for each unique MMSI
                rowsets are sorted by time
        '''
        # create query to crawl db
        qry = fcn(**self)

        # initialize db, run query
        print(qry)
        print('\nquerying the database...')
        aisdb = dbconn(dbpath)
        dt = datetime.now()
        aisdb.cur.execute(qry)
        delta =datetime.now() - dt
        print(f'query time: {delta.total_seconds():.2f}s')

        # get 100k rows at a time, yield sets of rows for each unique MMSI
        mmsi_rows = None
        while len(res := aisdb.cur.fetchmany(10**5)) > 0:
            if mmsi_rows is None:
                mmsi_rows = np.array(res, dtype=object)
            else:
                mmsi_rows = np.vstack((mmsi_rows, res))

            print(f'{mmsi_rows[0][0]}', end='\r')

            while len(mmsi_rows) > 1 and mmsi_rows[0][0] != mmsi_rows[-1][0]:
                if not isinstance(mmsi_rows[0][0], (float, int)):
                    print(f'error: MMSI not an integer! {mmsi_rows[0]}')
                    breakpoint()
                ummsi_idx = np.where(mmsi_rows[:,0] != mmsi_rows[0,0])[0][0]
                yield mmsi_rows[0:ummsi_idx]
                mmsi_rows = mmsi_rows[ummsi_idx:]

        yield mmsi_rows

        print('\ndone')

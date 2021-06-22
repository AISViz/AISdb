import os
from collections import UserDict
from functools import partial
from datetime import datetime
#import threading
import concurrent.futures

import numpy as np
from shapely.geometry import Polygon

import database
from database.lambdas import *
from database.qryfcn import *
from database.dbconn import dbconn


class qrygen(UserDict):
    '''  convert dictionary key:val pairs to SQL query code

    accepts query parameters as args, and stores them in a dictionary.
    some additional computed values are stored, e.g. bounding boxes and 
    polygon geometry when using coordinate arrays
    
    
    code example:
    ```
    from datetime import datetime

    from database.lambdas import *
    from database.qryfcn import *
    from database.dbconn import dbconn

    # create a database and then insert some rows
    cur = database.dbconn(dbpath=':memory:').cur
    database.create_table_msg123(cur, '202101')
    database.create_table_msg18(cur, '202101')
    database.create_table_msg5(cur, '202101')
    # insert rows using database.decoder or manually


    # these args will be passed to the query function and callback lambda to generate SQL code
    # for example, when using callback in_radius, xy must be a point, and a radius must be supplied in meters
    # times are specified using datetime.datetime() format
    qry = qrygen(
            xy=[-180, -90, -180, 90, 180, 90, 180, -90,],   # xy coordinate pairs
            # can also be specified as seperate arrays, e.g.
            # x=[-180, -180, 180, 180],
            # y=[-90, 90, 90, -90],
            start=datetime(2021,1,1),                       # start of query range 
            end=datetime(2021,2,1),                         # end of query range
        )

    sql = qry.crawl(callback=database.lambdas.in_poly, qryfcn=database.qryfcn.msg123union18join5)

    print(sql)
    cur.execute(sql)

    res = cur.fetchall()
    ```
    '''


    def __init__(self, **kwargs):

        self.data = kwargs

        if 'xy' in self.keys() and not 'x' in self.keys() and not 'y' in self.keys(): 
            self['x'] = self['xy'][::2]; self['y'] = self['xy'][1::2]

        if sum(map(lambda t: t in kwargs.keys(), ('start', 'end',))) == 2: 
            if isinstance(kwargs['start'], datetime):
                self.data.update({'months':dt2monthstr(**kwargs)})
            elif isinstance(kwargs['start'], (float, int)):
                self.data.update({'months':epoch2monthstr(**kwargs)})

        if 'x' in self.data.keys() and 'y' in self.data.keys():

            if sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2: 
                assert len(self['x']) == len(self['y']),                     'coordinate arrays are not equivalent length'
                assert Polygon(zip(self.data['x'],self.data['y'])).is_valid, 'invalid polygon'

                self.data['poly'] = arr2polytxt(x=self.data['x'], y=self.data['y'])

            else:
                assert 'radius' in self.keys(), 'undefined radius'


    def build_views(self, dbpath):
        aisdb = dbconn(dbpath)
        for month in self['months']:
            aisdb.cur.execute(f''' SELECT name FROM sqlite_master WHERE type='table' AND name='view_{month}_static' ''')
            if not [] == aisdb.cur.fetchall(): continue
            print(f'aggregating static messages 5, 18 into view_{month}_static...')
            aisdb.cur.execute(f'''
            CREATE TABLE IF NOT EXISTS view_{month}_static AS SELECT * FROM (
                SELECT m5.mmsi, m5.vessel_name, m5.ship_type, m5.dim_bow, m5.dim_stern, m5.dim_port, m5.dim_star, 
                COUNT(*) as n 
                  FROM ais_{month}_msg_5 AS m5
                  GROUP BY m5.mmsi, m5.ship_type, m5.vessel_name
                  HAVING n > 1
                UNION
                SELECT m24.mmsi, m24.vessel_name, m24.ship_type, m24.dim_bow, m24.dim_stern, m24.dim_port, m24.dim_star, 
                COUNT(*) as n
                  FROM ais_{month}_msg_24 AS m24
                  GROUP BY m24.mmsi, m24.ship_type, m24.vessel_name
                  HAVING n > 1
                ORDER BY 1 , 8 , 2 , 3 
            ) 
            GROUP BY mmsi
            HAVING MAX(n) > 1
            ''')

            aisdb.cur.execute(f''' CREATE UNIQUE INDEX IF NOT EXISTS idx_view_{month}_static ON 'view_{month}_static' (mmsi) ''')

            #aisdb.cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_shiptype ON 'ais_{month}_msg_5' (ship_type) ''')
            #aisdb.cur.execute(f''' CREATE INDEX IF NOT EXISTS idx_msg5_{month}_vesselname ON 'ais_{month}_msg_5' (vessel_name) ''')
            #aisdb.cur.execute(f''' SELECT * FROM view_{month}_static ''')
            #res = np.array(aisdb.cur.fetchall(), dtype=object)
            #aisdb.cur.execute(f''' SELECT count(*) FROM ais_{month}_msg_5''')
            #res = np.array(aisdb.cur.fetchall(), dtype=object)
            #aisdb.cur.execute(f''' DROP TABLE view_{month}_static ''')
            #aisdb.cur.execute(f''' DROP INDEX idx_msg5_{month}_vesselname ''')
            #aisdb.cur.execute(f''' DROP INDEX idx_msg5_{month}_shiptype ''')
        aisdb.conn.close()


    def crawl(self, callback, qryfcn=msg123union18join5):
        ''' returns an SQL query to crawl the database 
            query generated using given query function, parameters stored in self, and a callback function 
        '''
        return '\nUNION'.join(map(partial(qryfcn, callback=callback, kwargs=self), self['months'])) + '\nORDER BY 1, 9, 2'


    def crawl_unordered(self, callback, qryfcn=msg123union18join5):
        ''' returns an SQL query to crawl the database 
            query generated using given query function, parameters stored in self, and a callback function 
        '''
        return '\nUNION'.join(map(partial(qryfcn, callback=callback, kwargs=self), self['months']))


    def qry_thread(self, dbpath, qry):
        aisdb = dbconn(dbpath)
        aisdb.cur.execute(qry)
        return aisdb.cur.fetchall()


    def run_qry(self, dbpath, callback, qryfcn):
        qry = self.crawl(callback=callback, qryfcn=qryfcn)
        print(qry)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            aisdb = dbconn(dbpath)
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

        return np.array(res, dtype=object)


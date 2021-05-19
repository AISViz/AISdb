import os
from collections import UserDict
from functools import partial

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
            self.data.update({'months':dt2monthstr(**kwargs)})

        if 'x' in self.data.keys() and 'y' in self.data.keys():

            if sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2: 
                assert len(self['x']) == len(self['y']),                     'coordinate arrays are not equivalent length'
                assert Polygon(zip(self.data['x'],self.data['y'])).is_valid, 'invalid polygon'

                self.data['poly'] = arr2polytxt(x=self.data['x'], y=self.data['y'])

            else:
                assert 'radius' in self.keys(), 'undefined radius'


    def crawl(self, callback, qryfcn=msg123union18join5):
        ''' returns an SQL query to crawl the database 
            query generated using given query function, parameters stored in self, and a callback function 
        '''
        return '\nUNION'.join(map(partial(qryfcn, callback=callback, kwargs=self), self['months'])) + '\nORDER BY 1,2'



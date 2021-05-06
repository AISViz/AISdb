import os
from collections import UserDict
from functools import partial

from shapely.geometry import Polygon

import database
from database.lambdas import *
from database.qryfcn import *
from database.dbconn import dbconn


class qrygen(UserDict):
    '''
    __file__ = '/data/smith6/ais/src/qrygen.py'
    '''
    def __init__(self, **kwargs):
        self.data = kwargs
        if 'xy' in self.keys() and not 'x' in self.keys() and not 'y' in self.keys(): self['x'] = self['xy'][::2]; self['y'] = self['xy'][1::2]
        if sum(map(lambda t: t in kwargs.keys(), ('start', 'end',))) == 2: self.data.update({'months':dt2monthstr(**kwargs)})
        if 'x' in self.data.keys() and 'y' in self.data.keys():
            if sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2: 
                assert sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2, 'x,y are not 2D'
                assert len(self['x']) == len(self['y']), 'coordinate arrays are not equivalent length'
                assert Polygon(zip(self.data['x'],self.data['y'])).is_valid, 'invalid polygon'
                self.data['poly'] = arr2polytxt(x=self.data['x'], y=self.data['y'])
            else:
                assert 'radius' in self.keys(), 'undefined radius'

    def crawl(self, callback, qryfcn=msg123union18join5):
        return '\nUNION'.join(map(partial(qryfcn, callback=callback, kwargs=self), self['months']))

    def csvpath(self,subfolder,folder=os.path.abspath(f'..{os.path.sep}scripts{os.path.sep}')):
        return f'{folder}{os.path.sep}{subfolder}{os.path.sep if subfolder[-1] != os.path.sep else ""}ais_{self.data["start"].strftime("%Y%m%d")}-{self.data["end"].strftime("%Y%m%d")}{"_"+str(self["radius"] // 1000)+"km" if "radius" in self.data.keys() else ""}.csv'


if __name__ == '__main__':
    # example usage

    cur = database.dbconn(dbpath=':memory:').cur
    database.create_table_msg123(cur, '202101')
    database.create_table_msg18(cur, '202101')
    database.create_table_msg5(cur, '202101')

    # insert some data into the database using database.decoder or by inserting manually
    # then, 

    # these args will be passed to the query function and callback lambda to generate SQL code
    # for example, when using callback in_radius, xy must be a point, and a radius must be supplied in meters
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


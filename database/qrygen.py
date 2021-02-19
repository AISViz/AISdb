from collections import UserDict

from shapely.geometry import Polygon

from . import *

class qrygen(UserDict):
    '''
    __file__ = '/data/smith6/ais/query_postgres.py'
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
                #assert sum(map(isinstance, (self['x'],self['y'],), [(float, int) for _ in range(2)])) == 2, 'x,y are not 1D'
                assert 'radius' in self.keys(), 'undefined radius'

    def radial_msg123join5(self, callback=in_radius2):
        ''' union on monthly tables for msg123 join msg5 on distinct mmsis '''
        assert 'radius' in self.keys(), 'undefined radius'
        return '\nUNION'.join(map(partial(msg123join5, callback=callback, kwargs=self), self['months'])) + '\nORDER BY mmsi, time'

    def radial_msg123union18join5(self, callback=in_radius2):
        ''' union on monthly tables for msg123 join msg5 on distinct mmsis '''
        assert 'radius' in self.keys(), 'undefined radius'
        return '\nUNION'.join(map(partial(msg123union18join5, callback=callback, kwargs=self), self['months'])) + '\nORDER BY mmsi, time'

    def poly_msg123join5(self, callback=in_poly):
        assert sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2, 'x,y are not 2D'
        return '\nUNION'.join(map(partial(msg123join5, callback=callback, kwargs=self), self['months'])) + '\nORDER BY mmsi, time'

    def poly_msg123union18join5(self, callback=in_poly):
        assert sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2, 'x,y are not 2D'
        return '\nUNION'.join(map(partial(msg123union18join5, callback=callback, kwargs=self), self['months'])) + '\nORDER BY mmsi, time'

    def poly_msg18join5(self, callback=in_poly):
        assert sum(map(isinstance, (self['x'],self['y'],), [(list, np.ndarray, tuple) for _ in range(2)])) == 2, 'x,y are not 2D'
        return '\nUNION'.join(map(partial(msg18join5, callback=callback, kwargs=self), self['months'])) + '\nORDER BY mmsi, time'

    def crawler(self, callback, qryfcn=msg123union18join5):
        return '\nUNION'.join(map(partial(qryfcn, callback=callback, kwargs=self), self['months'])) + '\nORDER BY mmsi, time'

    def csvpath(self,subfolder,folder=f'{os.path.dirname(__file__)}{os.path.sep}scripts{os.path.sep}'):
        return f'{folder}{subfolder}{os.path.sep if subfolder[-1] != os.path.sep else ""}ais_{self.data["start"].strftime("%Y%m%d")}-{self.data["end"].strftime("%Y%m%d")}{"_"+str(self["radius"] // 1000)+"km" if "radius" in self.data.keys() else ""}.csv'

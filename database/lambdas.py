import __main__
from datetime import datetime, timedelta

import numpy as np

from database.dbconn import dbconn

for key, val in dbconn().lambdas.items(): setattr(__main__, key, val)

in_radius = __main__.in_radius
in_poly = __main__.in_poly
in_radius_time = __main__.in_radius_time
in_bbox = __main__.in_bbox


dt2monthstr = lambda start, end, **_: np.unique([t.strftime('%Y%m') for t in np.arange(start, end, timedelta(days=1)).astype(datetime) ]).astype(str)

zipcoords   = lambda x,y,**_: ', '.join(map(lambda xi,yi: f'{xi} {yi}', x,y))

arr2polytxt = lambda x,y,**_: f'POLYGON(({zipcoords(x,y)}))'

boxpoly = lambda x,y: ([min(x), min(x), max(x), max(x), min(x)], [min(y), max(y), max(y), min(y), min(y)])

merge = lambda *arr: np.concatenate(np.array(*arr).T)

valid_mmsi = lambda alias='m123',**_: f'{alias}.mmsi >= 201000000 AND {alias}.mmsi < 776000000'

in_poly_validmmsi = lambda **kwargs: f'{valid_mmsi(**kwargs)} AND {in_poly(**kwargs)}' 


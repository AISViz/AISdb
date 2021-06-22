import __main__
from datetime import datetime, timedelta

import numpy as np

from database.dbconn import dbconn
from database import epoch_2_dt

for key, val in dbconn().lambdas.items(): setattr(__main__, key, val)

in_radius = __main__.in_radius
in_poly = __main__.in_poly
in_radius_time = __main__.in_radius_time
in_bbox = __main__.in_bbox


dt2monthstr = lambda start, end, **_: np.unique([t.strftime('%Y%m') for t in np.arange(start, end, timedelta(days=1)).astype(datetime) ]).astype(str)
epoch2monthstr = lambda start, end, **_: dt2monthstr(epoch_2_dt(start), epoch_2_dt(end))

zipcoords   = lambda x,y,**_: ', '.join(map(lambda xi,yi: f'{xi} {yi}', x,y))

arr2polytxt = lambda x,y,**_: f'POLYGON(({zipcoords(x,y)}))'

boxpoly = lambda x,y: ([min(x), min(x), max(x), max(x), min(x)], [min(y), max(y), max(y), min(y), min(y)])

merge = lambda *arr: np.concatenate(np.array(*arr).T)

valid_mmsi = lambda alias='m123',**_: f'{alias}.mmsi >= 201000000 AND {alias}.mmsi < 776000000'

in_poly_validmmsi = lambda **kwargs: f'{valid_mmsi(**kwargs)} AND {in_poly(**kwargs)}' 

in_timerange = lambda **kwargs: f'''{kwargs['alias']}.time BETWEEN date('{kwargs['start'].strftime('%Y-%m-%d')}') AND date('{kwargs['end'].strftime('%Y-%m-%d')}')'''

in_time_mmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)}'

#in_poly_time_mmsi = lambda **kwargs: f'{in_poly(**kwargs)} AND {in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)}'
in_time_poly_mmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {in_poly(**kwargs)} AND {valid_mmsi(**kwargs)}'

in_time_poly = lambda **kwargs: f'{in_timerange(**kwargs)} AND {in_poly(**kwargs)}'

from database.decoder import dt_2_epoch


rtree_in_timerange = lambda **kwargs: f''' 
        {kwargs['alias']}.t0 >= {dt_2_epoch(kwargs['start'])} AND 
        {kwargs['alias']}.t1 <= {dt_2_epoch(kwargs['end'])}'''

rtree_valid_mmsi = lambda alias='m123',**_: f'''
        {alias}.mmsi0 >= 201000000 AND 
        {alias}.mmsi1 < 776000000'''

rtree_in_time_mmsi = lambda **kwargs: f''' 
        {rtree_in_timerange(**kwargs)} AND {rtree_valid_mmsi(**kwargs)}'''

rtree_in_bbox = lambda alias, **kwargs:(f'''
        {alias}.x0 >= {kwargs['xmin']} AND
        {alias}.x1 <= {kwargs['xmax']} AND
        {alias}.y0 >= {kwargs['ymin']} AND 
        {alias}.y1 <= {kwargs['ymax']} ''')

rtree_in_time_bbox_mmsi = lambda **kwargs: f''' {rtree_in_timerange(**kwargs)} AND {rtree_in_bbox(**kwargs)} AND {rtree_valid_mmsi(**kwargs)} '''
rtree_in_bbox_time_mmsi = lambda **kwargs: f''' {rtree_in_bbox(**kwargs)} AND {rtree_in_timerange(**kwargs)} AND {rtree_valid_mmsi(**kwargs)} '''


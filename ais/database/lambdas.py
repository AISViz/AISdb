import __main__
from datetime import datetime, timedelta

import numpy as np

from database.dbconn import dbconn
#from database import epoch_2_dt
from gis import epoch_2_dt, dt_2_epoch

'''
the following code block will set implementation-specific lambdas according to 
the implementation selected in class dbconn. see dbconn.py for more info
'''
for key, val in dbconn().lambdas.items(): setattr(__main__, key, val)
in_radius = __main__.in_radius
in_poly = __main__.in_poly
in_radius_time = __main__.in_radius_time
in_bbox = __main__.in_bbox


'''
collection of callback functions for dynamically generating SQL queries,
as well as some general utility one-liners, e.g. zipping XY coordinate arrays 
for WKT geometry parsing
'''

dt2monthstr = lambda start, end, **_: np.unique([t.strftime('%Y%m') for t in np.arange(start, end, timedelta(days=1)).astype(datetime) ]).astype(str)
epoch2monthstr = lambda start, end, **_: dt2monthstr(epoch_2_dt(start), epoch_2_dt(end))

zipcoords   = lambda x,y,**_: ', '.join(map(lambda xi,yi: f'{xi} {yi}', x,y))

arr2polytxt = lambda x,y,**_: f'POLYGON(({zipcoords(x,y)}))'

boxpoly = lambda x,y: ([min(x), min(x), max(x), max(x), min(x)], [min(y), max(y), max(y), min(y), min(y)])

merge = lambda *arr: np.concatenate(np.array(*arr).T)



# query intermediary tables
valid_mmsi = lambda alias='m123',**_: f'{alias}.mmsi >= 201000000 AND {alias}.mmsi < 776000000'
has_mmsi = lambda alias, mmsi, **_: f'{alias}.mmsi = {str(mmsi)}'
in_mmsi = lambda alias, mmsis, **_: f'{alias}.mmsi IN ({", ".join(map(str, mmsis))})'

in_timerange = lambda **kwargs: f'''{kwargs['alias']}.time BETWEEN date('{kwargs['start'].strftime('%Y-%m-%d')}') AND date('{kwargs['end'].strftime('%Y-%m-%d')}')'''

in_time_poly = lambda **kwargs: f'{in_timerange(**kwargs)} AND {in_poly(**kwargs)}'
in_poly_validmmsi = lambda **kwargs: f'{valid_mmsi(**kwargs)} AND {in_poly(**kwargs)}' 
in_time_poly_validmmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {in_poly(**kwargs)} AND {valid_mmsi(**kwargs)}'


# query rtree table index; same as above, however
#   mmsi column replaced with mmsi0, mmsi1 columns
#   time column replaced with t0, t1 columns (as epoch time)
#   longitude column replaced with x0, x1 columns
#   latitude column replaced with y0,y1 columns



rtree_in_timerange = lambda **kwargs: f''' 
        {kwargs['alias']}.t0 >= {dt_2_epoch(kwargs['start'])} AND 
        {kwargs['alias']}.t1 <= {dt_2_epoch(kwargs['end'])}'''

rtree_has_mmsi      = lambda alias, mmsi, **_: f'''
        CAST({alias}.mmsi0 AS INT) = {mmsi} '''

rtree_in_mmsi       = lambda alias, mmsis, **_: f'''
        {alias}.mmsi0 IN ({", ".join(map(str, mmsis))}) '''

rtree_valid_mmsi    = lambda alias='m123', **_: f'''
        {alias}.mmsi0 >= 201000000 AND 
        {alias}.mmsi1 < 776000000 '''

rtree_in_time_mmsi = lambda **kwargs: f''' 
        {rtree_in_timerange(**kwargs)} AND {rtree_valid_mmsi(**kwargs)}'''

rtree_in_bbox = lambda alias, **kwargs:(f'''
        {alias}.x0 >= {kwargs['xmin']} AND
        {alias}.x1 <= {kwargs['xmax']} AND
        {alias}.y0 >= {kwargs['ymin']} AND 
        {alias}.y1 <= {kwargs['ymax']} ''')

rtree_in_time_bbox = lambda **kwargs: f''' {rtree_in_timerange(**kwargs)} AND {rtree_in_bbox(**kwargs)} '''
rtree_in_bbox_time = lambda **kwargs: f''' {rtree_in_bbox(**kwargs)} AND {rtree_in_timerange(**kwargs)} '''
rtree_in_validmmsi_bbox = lambda **kwargs: f''' {rtree_valid_mmsi(**kwargs)} AND {rtree_in_bbox(**kwargs)} '''

rtree_in_time_bbox_validmmsi = lambda **kwargs: f''' {rtree_in_timerange(**kwargs)} AND {rtree_in_bbox(**kwargs)} AND {rtree_valid_mmsi(**kwargs)} '''
rtree_in_bbox_time_validmmsi = lambda **kwargs: f''' {rtree_in_bbox(**kwargs)} AND {rtree_in_timerange(**kwargs)} AND {rtree_valid_mmsi(**kwargs)} '''

rtree_in_timerange_hasmmsi = lambda **kwargs: f'{rtree_in_timerange(**kwargs)} AND {rtree_has_mmsi(**kwargs)}'
rtree_in_timerange_inmmsi = lambda **kwargs: f'{rtree_in_timerange(**kwargs)} AND {rtree_in_mmsi(**kwargs)}'
rtree_in_timerange_validmmsi = lambda **kwargs: f'{rtree_in_timerange(**kwargs)} AND {rtree_valid_mmsi(**kwargs)}'

rtree_in_time_bbox_hasmmsi = lambda **kwargs: f'''
        {rtree_in_timerange(**kwargs)} AND 
        {rtree_in_bbox(**kwargs)} AND 
        {rtree_has_mmsi(**kwargs)}'''

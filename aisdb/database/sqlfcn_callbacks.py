from datetime import datetime, timedelta

import numpy as np

from gis import epoch_2_dt, dt_2_epoch
# from common import table_prefix

# utility functions

dt2monthstr = lambda start, end, **_: np.unique([
    t.strftime('%Y%m')
    for t in np.arange(start, end, timedelta(days=1)).astype(datetime)
]).astype(object)

epoch2monthstr = lambda start, end, **_: dt2monthstr(epoch_2_dt(start),
                                                     epoch_2_dt(end))
zipcoords = lambda x, y, **_: ', '.join(map(lambda xi, yi: f'{xi} {yi}', x, y))

arr2polytxt = lambda x, y, **_: f'POLYGON(({zipcoords(x,y)}))'

boxpoly = lambda x, y: ([
    min(x), min(x), max(x), max(x), min(x)
], [min(y), max(y), max(y), min(y), min(y)])

merge = lambda *arr: np.concatenate(np.array(*arr).T)

# callback functions

in_bbox = lambda alias, **kwargs: (f'''
        {alias}.longitude >= {kwargs['xmin']} AND
        {alias}.longitude <= {kwargs['xmax']} AND
        {alias}.latitude >= {kwargs['ymin']} AND
        {alias}.latitude <= {kwargs['ymax']} ''')

in_timerange = lambda **kwargs: f'''
        {kwargs['alias']}.time >= {dt_2_epoch(kwargs['start'])} AND
        {kwargs['alias']}.time <= {dt_2_epoch(kwargs['end'])}'''

has_mmsi = lambda alias, mmsi, **_: f'''
        CAST({alias}.mmsi AS INT) = {mmsi} '''

in_mmsi = lambda alias, mmsis, **_: f'''
        {alias}.mmsi IN ({", ".join(map(str, mmsis))}) '''

valid_mmsi = lambda alias='m123', **_: f'''
        {alias}.mmsi >= 201000000 AND
        {alias}.mmsi < 776000000 '''

in_time_mmsi = lambda **kwargs: f'''
        {in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)}'''

in_timerange_validmmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)}'

in_time_bbox = lambda **kwargs: f''' {in_timerange(**kwargs)} AND {in_bbox(**kwargs)} '''
in_bbox_time = lambda **kwargs: f''' {in_bbox(**kwargs)} AND {in_timerange(**kwargs)} '''
in_validmmsi_bbox = lambda **kwargs: f''' {valid_mmsi(**kwargs)} AND {in_bbox(**kwargs)} '''

in_time_bbox_validmmsi = lambda **kwargs: f''' {in_timerange(**kwargs)} AND {in_bbox(**kwargs)} AND {valid_mmsi(**kwargs)} '''
in_bbox_time_validmmsi = lambda **kwargs: f''' {in_bbox(**kwargs)} AND {in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)} '''

in_timerange_hasmmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {has_mmsi(**kwargs)}'
in_timerange_inmmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {in_mmsi(**kwargs)}'
in_timerange_validmmsi = lambda **kwargs: f'{in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)}'

in_time_bbox_hasmmsi = lambda **kwargs: f'''
        {in_timerange(**kwargs)} AND
        {in_bbox(**kwargs)} AND
        {has_mmsi(**kwargs)}'''

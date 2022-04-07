from datetime import datetime, timedelta

import numpy as np

from gis import epoch_2_dt, dt_2_epoch, shiftcoord
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
def in_bbox(alias, *, xmin, xmax, ymin, ymax, **_):
    x0 = shiftcoord([xmin])[0]
    x1 = shiftcoord([xmax])[0]
    if x0 <= x1:
        return f'''
        {alias}.longitude >= {x0} AND
        {alias}.longitude <= {x1} AND
        {alias}.latitude >= {ymin} AND
        {alias}.latitude <= {ymax} '''
    else:
        return f'''
        ({alias}.longitude >= {x0} OR {alias}.longitude <= {x1}) AND
        {alias}.latitude >= {ymin} AND
        {alias}.latitude <= {ymax} '''


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

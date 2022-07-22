''' redefinitions of functions in :py:mod:`aisdb.database.sql_query_strings`,
    combined into lambdas for convenience
'''

from datetime import datetime, timedelta

import numpy as np

from aisdb.database.sql_query_strings import *

dt2monthstr = lambda start, end, **_: np.unique([
    t.strftime('%Y%m')
    for t in np.arange(start, end, timedelta(days=1)).astype(datetime)
]).astype(object)
'''
zipcoords = lambda x, y, **_: ', '.join(map(lambda xi, yi: f'{xi} {yi}', x, y))

arr2polytxt = lambda x, y, **_: f'POLYGON(({zipcoords(x,y)}))'

boxpoly = lambda x, y: ([
    min(x), min(x), max(x), max(x), min(x)
], [min(y), max(y), max(y), min(y), min(y)])

merge = lambda *arr: np.concatenate(np.array(*arr).T)
'''

in_time_mmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND {valid_mmsi(**kwargs)}'''

in_timerange_validmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {valid_mmsi(**kwargs)}'''

in_time_bbox = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {in_bbox(**kwargs)} '''

in_bbox_time = lambda **kwargs: f'''\
    {in_bbox(**kwargs)} AND
    {in_timerange(**kwargs)} '''

in_validmmsi_bbox = lambda **kwargs: f'''\
    {valid_mmsi(**kwargs)} AND
    {in_bbox(**kwargs)} '''

in_time_bbox_inmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {in_bbox(**kwargs)} AND
    {in_mmsi(**kwargs)} '''

in_time_bbox_validmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {in_bbox(**kwargs)} AND
    {valid_mmsi(**kwargs)} '''

in_bbox_time_validmmsi = lambda **kwargs: f'''\
    {in_bbox(**kwargs)} AND
    {in_timerange(**kwargs)} AND
    {valid_mmsi(**kwargs)} '''

in_timerange_hasmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND {has_mmsi(**kwargs)}'''

in_timerange_inmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {in_mmsi(**kwargs)}'''

in_timerange_validmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {valid_mmsi(**kwargs)} '''

in_time_bbox_hasmmsi = lambda **kwargs: f'''\
    {in_timerange(**kwargs)} AND
    {in_bbox(**kwargs)} AND
    {has_mmsi(**kwargs)}'''

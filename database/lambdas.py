from datetime import datetime, timedelta

import numpy as np


dt2monthstr = lambda start, end, **_: np.unique([t.strftime('%Y%m') for t in np.arange(start, end, timedelta(days=1)).astype(datetime) ]).astype(str)

zipcoords   = lambda x,y,**_: ', '.join(map(lambda xi,yi: f'{xi} {yi}', x,y))

arr2polytxt = lambda x,y,**_: f'POLYGON(({zipcoords(x,y)}))'

in_poly     = lambda poly,alias='m123',**_: f'ST_Contains(\n    ST_GeomFromText(\'{poly}\'),\n    ST_MakePoint({alias}.longitude, {alias}.latitude)\n  )'
in_poly_spatialite= lambda poly,alias='m123',**_: f'Contains(\n    GeomFromText(\'{poly}\'),\n    MakePoint({alias}.longitude, {alias}.latitude)\n  )'

boxpoly = lambda x,y: ([min(x), min(x), max(x), max(x), min(x)], [min(y), max(y), max(y), min(y), min(y)])


# radius distance unit in meters
in_radius   = lambda month,*,x,y,radius,**_: f'ST_DWithin(Geography(ais_s_{month}_msg_1_2_3.ais_geom), Geography(ST_MakePoint({x}, {y})), {radius})'
in_radius2  = lambda *,x,y,radius,**_: f'ST_DWithin(Geography(m123.ais_geom), Geography(ST_MakePoint({x}, {y})), {radius})'
in_radius_time  = lambda *,x,y,radius,alias='m123',**kwargs: f'ST_DWithin(Geography({alias}.ais_geom), Geography(ST_MakePoint({x}, {y})), {radius}) AND {alias}.time BETWEEN \'{kwargs["start"].strftime("%Y-%m-%d %H:%M:%S")}\'::date AND \'{kwargs["end"].strftime("%Y-%m-%d %H:%M:%S")}\'::date'


in_bbox     = lambda south, north, west, east,**_:    f'ais_geom && ST_MakeEnvelope({west},{south},{east},{north})'


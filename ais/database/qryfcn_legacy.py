import os
import logging

#prefix = 'ais_'
#if os.environ.get('POSTGRESDB'): 
#    prefix = 'ais_s_'  # legacy database
from common import table_prefix
prefix = table_prefix 


msg123union18join5 = lambda month, callback, kwargs: (f'''
SELECT m123.mmsi, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM {prefix}{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM {prefix}{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m18', **kwargs)}
  --GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
--ORDER BY 1, 2 ''')


rtree_nogeom = lambda month, callback, kwargs, bounds=dict(xmin=-180, ymin=-90, xmax=180, ymax=90): (f'''
SELECT m123.mmsi, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM {prefix}{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m123.longitude >= {kwargs['xmin']}
  AND m123.longitude <= {kwargs['xmax']}
  AND m123.latitude >= {kwargs['ymin']}
  AND m123.latitude <= {kwargs['ymax']}
  AND {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM {prefix}{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m18.longitude >= {kwargs['xmin']}
  AND m18.longitude <= {kwargs['xmax']}
  AND m18.latitude >= {kwargs['ymin']}
  AND m18.latitude <= {kwargs['ymax']}
  AND {callback(month=month, alias='m18', **kwargs)}
ORDER BY 1, 2
  ''')


imotest = lambda month, callback, kwargs: (f'''
SELECT m123.mmsi, m5.imo, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM {prefix}{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type, x.imo
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name, x.imo
      HAVING COUNT(*) > 1
  ) AS m5 ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi, m5.imo, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM {prefix}{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type, x.imo
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name, x.imo
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m18', **kwargs)}
  GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt, m5.imo
  ''')


scan_static = lambda month, callback, kwargs: (f'''
SELECT DISTINCT mmsi, imo, vessel_name, ship_type
  FROM {prefix}{month}_msg_5 AS m5
  WHERE {callback(month=month, alias='m5', **kwargs)}
UNION
SELECT DISTINCT mmsi, imo, vessel_name, ship_type
  FROM {prefix}{month}_msg_24 AS m24 
  WHERE {callback(month=month, alias='m24', **kwargs)}
  ''')


rtree_nogeom = lambda month, callback, kwargs=dict(west=-180, south=-90, east=180, north=90): (f'''
SELECT m123.mmsi0, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON m123.mmsi0 = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m123.x0 >= {kwargs['west']}
  AND m123.x1 <= {kwargs['east']}
  AND m123.y0 >= {kwargs['south']}
  AND m123.y1 <= {kwargs['north']}
  AND {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi0, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON m18.mmsi0 = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m18.x0 >= {kwargs['west']}
  AND m18.x1 <= {kwargs['east']}
  AND m18.y0 >= {kwargs['south']}
  AND m18.y1 <= {kwargs['north']}
  AND {callback(month=month, alias='m18', **kwargs)}
ORDER BY 1, 2
  ''')


rtree_minified = lambda month, callback, kwargs: (f'''
SELECT CAST(m123.mmsi0 AS INT), m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON CAST(m123.mmsi0 AS INT) = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT CAST(m18.mmsi0 AS INT), m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM rtree_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM {prefix}{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 
  ON CAST(m18.mmsi0 AS INT) = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m18', **kwargs)}
--ORDER BY 1, 2
  ''')

# join upon position reports and static vessel aggregate data
leftjoin_dynamic_static = lambda month, callback, kwargs: (f'''
WITH dynamic_{month} AS ( {rtree_dynamic(month, callback, kwargs)} 
),
static_{month} AS ( {static(month)} 
)
SELECT dynamic_{month}.mmsi, dynamic_{month}.t0, 
        dynamic_{month}.x0, dynamic_{month}.y0, 
        dynamic_{month}.cog, dynamic_{month}.sog, 
        dynamic_{month}.msgtype, 
        static_{month}.imo, static_{month}.vessel_name,
        static_{month}.dim_bow, static_{month}.dim_stern, 
        static_{month}.dim_port, static_{month}.dim_star,
        static_{month}.ship_type, ref.coarse_type_txt 
    FROM dynamic_{month} 
LEFT JOIN static_{month}
    ON dynamic_{month}.mmsi = static_{month}.mmsi
LEFT JOIN coarsetype_ref AS ref 
    ON (static_{month}.ship_type = ref.coarse_type) ''')


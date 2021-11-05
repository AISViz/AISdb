import os
import logging

prefix = 'ais_'
if os.environ.get('POSTGRESDB'): 
    prefix = 'ais_s_'  # legacy database
    logging.warning('WARNING: using non-standard table name prefix')

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


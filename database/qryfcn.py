from database.qryfcn_legacy import *


rtree_nogeom = lambda month, callback, kwargs, bounds=dict(xmin=-180, ymin=-90, xmax=180, ymax=90): (f'''
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
  WHERE m123.x0 >= {kwargs['xmin']}
  AND m123.x1 <= {kwargs['xmax']}
  AND m123.y0 >= {kwargs['ymin']}
  AND m123.y1 <= {kwargs['ymax']}
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
  WHERE m18.x0 >= {kwargs['xmin']}
  AND m18.x1 <= {kwargs['xmax']}
  AND m18.y0 >= {kwargs['ymin']}
  AND m18.y1 <= {kwargs['ymax']}
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



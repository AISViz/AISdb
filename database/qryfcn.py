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



# query a union of position reports using rtree indexes
rtree_dynamic = lambda month, callback, kwargs: (f'''
    SELECT CAST(m123.mmsi0 AS INT) as mmsi, m123.t0, m123.x0, m123.y0, m123.cog, m123.sog, m123.msgtype
      FROM rtree_{month}_msg_1_2_3 AS m123
      WHERE {callback(month=month, alias='m123', **kwargs)}
    UNION
    SELECT CAST(m18.mmsi0 AS INT) as mmsi, m18.t0, m18.x0, m18.y0, m18.cog, m18.sog, 18 as msgtype
      FROM rtree_{month}_msg_18 AS m18
      WHERE {callback(month=month, alias='m18', **kwargs)} ''')


# static table views are generated at query-time using CREATE TABLE IF NOT EXISTS... 
# see build_views function in qrygen.py 
static = lambda month, **_: (f'''
    SELECT mmsi, vessel_name, ship_type, dim_bow, dim_stern, dim_port, dim_star FROM view_{month}_static ''')



leftjoin_dynamic_static = lambda month, callback, kwargs: (f'''
WITH dynamic_{month} AS ( {rtree_dynamic(month, callback, kwargs)} 
),
static_{month} AS ( {static(month)} 
)
SELECT dynamic_{month}.mmsi, dynamic_{month}.t0, 
        dynamic_{month}.x0, dynamic_{month}.y0, 
        dynamic_{month}.cog, dynamic_{month}.sog, 
        static_{month}.vessel_name, ref.coarse_type_txt, 
        dynamic_{month}.msgtype, static_{month}.imo,
        static_{month}.dim_bow, static_{month}.dim_stern, 
        static_{month}.dim_port, static_{month}.dim_star
    FROM dynamic_{month} LEFT JOIN static_{month}
      ON dynamic_{month}.mmsi = static_{month}.mmsi
    LEFT JOIN coarsetype_ref AS ref 
      ON (static_{month}.ship_type = ref.coarse_type) ''')


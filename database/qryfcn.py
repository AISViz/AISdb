msg123 = lambda args: (f'''
SELECT mmsi, time, longitude, latitude, sog, cog 
  FROM ais_s_{args['month']}_msg_1_2_3 as x
  WHERE { args['callback'](**args['kwargs']) } 
  GROUP BY mmsi, time, longitude, latitude, sog, cog \n''')

msgtype = lambda month, kwargs, msgnum: (f'''
SELECT * FROM ais_s_{month}_msg_{msgnum} as x
  WHERE mmsi >= 201000000 AND mmsi < 776000000 AND {in_bbox(**kwargs)}''')

msg5 = lambda poly, month: (f'''
SELECT unq_id_prefix, lineno, errorflag, mmsi, message_id, repeat_indicator, time, millisecond, region, country, base_station, online_data, group_code, sequence_id, channel, data_length, vessel_name, call_sign, imo, ship_type, dim_bow, dim_stern, dim_port, dim_star, draught, destination, ais_version, fixing_device, trans_control, eta_month, eta_day, eta_hour, eta_minute, sequence, dte, mode, spare, spare2
  FROM ais_s_{month}_msg_5 as x
  WHERE { in_poly(poly) } AND x.mmsi >= 201000000 AND x.mmsi < 776000000
  ORDER BY x.mmsi
''')

msg123join5 = lambda month, callback, kwargs: (f'''
SELECT m123.mmsi, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m123.mmsi >= 201000000 
    AND m123.mmsi <  776000000 
    --AND in_poly(poly)
    AND {callback(month=month, **kwargs)}
  GROUP BY m123.mmsi, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt''')

"""
msg123union18join5 = lambda month, callback, kwargs: (f'''
SELECT m123.mmsi, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m123.mmsi >= 201000000 
    AND m123.mmsi <  776000000 
    AND {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m18.mmsi >= 201000000 
    AND m18.mmsi <  776000000 
    AND {callback(month=month, alias='m18', **kwargs)}
  GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  ''')
"""

msg18join5 = lambda month, callback, kwargs: (f'''
SELECT m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m18.mmsi >= 201000000 
    AND m18.mmsi <  776000000 
    AND {callback(month=month, alias='m18', **kwargs)}
  GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  ''')

msg18 = lambda month, callback, kwargs: (f'''
SELECT m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE m18.mmsi >= 201000000 
    AND m18.mmsi <  776000000 
    AND {callback(month=month, alias='m18', **kwargs)}
  GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  ''')

count_box = lambda month, callback, **kwargs: (f'''
SELECT count(*)
  FROM ais_s_{month}_msg_1_2_3 as x
  WHERE { callback(month, **kwargs) } ''')

count_poly= lambda month, poly: (f'''
SELECT count(*)
  FROM ais_s_{month}_msg_1_2_3 as x
  WHERE { in_poly(poly) } ''')

msg123union18join5 = lambda month, callback, kwargs: (f'''
SELECT m123.mmsi, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m18', **kwargs)}
  GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  ''')

imotest = lambda month, callback, kwargs: (f'''
SELECT m123.mmsi, m5.imo, m123.time, m123.longitude, m123.latitude, m123.cog, m123.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_1_2_3 AS m123
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type, x.imo
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name, x.imo
      HAVING COUNT(*) > 1
  ) AS m5 ON m123.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m123', **kwargs)}
UNION
SELECT m18.mmsi, m5.imo, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt
  FROM ais_s_{month}_msg_18 AS m18
  LEFT JOIN (
    SELECT DISTINCT x.mmsi, x.vessel_name, x.ship_type, x.imo
      FROM ais_s_{month}_msg_5 AS x
      GROUP BY x.mmsi, x.ship_type, x.vessel_name, x.imo
      HAVING COUNT(*) > 1
  ) AS m5 ON m18.mmsi = m5.mmsi
  LEFT JOIN coarsetype_ref AS ref ON (m5.ship_type = ref.coarse_type)
  WHERE {callback(month=month, alias='m18', **kwargs)}
  GROUP BY m18.mmsi, m18.time, m18.longitude, m18.latitude, m18.cog, m18.sog, m5.vessel_name, ref.coarse_type_txt, m5.imo
  ''')

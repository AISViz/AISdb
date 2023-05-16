  SELECT 
    d.mmsi, 
    d.time, 
    d.longitude,
    d.latitude,
    d.sog,
    d.cog
  FROM ais_{}_dynamic AS d
  WHERE

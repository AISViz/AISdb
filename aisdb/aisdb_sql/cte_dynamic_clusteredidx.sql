  SELECT 
    d.mmsi, 
    d.time,
    d.longitude,
    d.latitude,
    d.rot,
    d.sog,
    d.cog,
    d.heading,
    d.maneuver
  FROM ais_{}_dynamic AS d
  WHERE

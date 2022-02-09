    SELECT 
      d.mmsi, 
      d.time, 
      d.longitude,
      d.latitude
    FROM ais_{}_dynamic AS d
    WHERE


INSERT INTO rtree_{}_dynamic (
        mmsi0, mmsi1, 
        t0, t1,
        x0, x1, 
        y0, y1,
        rot, 
        sog, 
        cog,
        heading, 
        utc_second,
        source
) SELECT 
        mmsi, mmsi, 
        time, time,
        longitude, longitude,
        latitude, latitude,
        rot,
        sog,
        cog,
        heading,
        utc_second,
        source
  FROM ais_{}_dynamic
  ORDER BY 1, 3;

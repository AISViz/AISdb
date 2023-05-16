  SELECT 
    s.mmsi, 
    TRIM(s.vessel_name) as vessel_name, 
    s.ship_type,
    s.dim_bow, 
    s.dim_stern, 
    s.dim_port, 
    s.dim_star, 
    s.imo
  FROM ais_{}_static AS s

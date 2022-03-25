
  SELECT 
    mmsi, 
    TRIM(vessel_name) as vessel_name, 
    ship_type,
    dim_bow, 
    dim_stern, 
    dim_port, 
    dim_star, 
    imo
  FROM static_{}_aggregate

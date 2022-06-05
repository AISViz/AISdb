
  SELECT 
    agg.mmsi, 
    TRIM(agg.vessel_name) as vessel_name, 
    agg.ship_type,
    agg.dim_bow, 
    agg.dim_stern, 
    agg.dim_port, 
    agg.dim_star, 
    agg.imo
  FROM static_{}_aggregate AS agg

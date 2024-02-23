  SELECT 
    agg.mmsi, 
    TRIM(agg.vessel_name) as vessel_name, 
    agg.ship_type,
    agg.imo,
    agg.dim_bow,
    agg.dim_stern,
    agg.dim_port,
    agg.dim_star,
    agg.draught,
    agg.destination,
    agg.eta_month,
    agg.eta_day,
    agg.eta_hour,
    agg.eta_minute
  FROM static_{}_aggregate AS agg

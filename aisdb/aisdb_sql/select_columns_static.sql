SELECT
    s.mmsi,
    s.imo,
    TRIM(vessel_name) as vessel_name,
    s.ship_type,
    s.call_sign,
    s.dim_bow,
    s.dim_stern,
    s.dim_port,
    s.dim_star,
    s.draught, 
    s.destination,
    s.eta_month, 
    s.eta_hour,
    s.eta_day, 
    s.eta_minute
FROM ais_{}_static AS s
WHERE s.mmsi = $7 ;

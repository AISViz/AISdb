SELECT 
    dynamic.mmsi, 
    dynamic.time,
    dynamic.utc_second,
    dynamic.longitude,
    dynamic.latitude,
    dynamic.rot,
    dynamic.sog,
    dynamic.cog,
    dynamic.heading,
    dynamic.maneuver,
    static.imo,
    static.ship_type,
    static.vessel_name,
    static.dim_bow,
    static.dim_stern,
    static.dim_port,
    static.dim_star,
    static.draught,
    static.destination,
    static.eta_month,
    static.eta_day,
    static.eta_hour,
    static.eta_minute,
    ref.coarse_type_txt AS ship_type_txt
FROM ais_global_dynamic AS dynamic
LEFT JOIN ais_global_static AS static ON d.mmsi = static.mmsi
LEFT JOIN ref ON static.ship_type = ref.coarse_type

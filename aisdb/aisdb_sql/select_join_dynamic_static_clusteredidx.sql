SELECT 
    dynamic_{}.mmsi, 
    dynamic_{}.time,
    dynamic_{}.longitude,
    dynamic_{}.latitude,
    dynamic_{}.rot,
    dynamic_{}.sog,
    dynamic_{}.cog,
    dynamic_{}.heading,
    dynamic_{}.maneuver,
    static_{}.imo,
    static_{}.ship_type,
    static_{}.vessel_name,
    static_{}.dim_bow,
    static_{}.dim_stern,
    static_{}.dim_port,
    static_{}.dim_star,
    static_{}.draught,
    static_{}.destination,
    static_{}.eta_month,
    static_{}.eta_day,
    static_{}.eta_hour,
    static_{}.eta_minute,
    ref.coarse_type_txt AS ship_type_txt
  FROM dynamic_{}
  LEFT JOIN static_{} ON 
    dynamic_{}.mmsi = static_{}.mmsi
  LEFT JOIN ref ON 
    static_{}.ship_type = ref.coarse_type

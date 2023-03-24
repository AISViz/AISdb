SELECT 
    dynamic_{}.mmsi, 
    dynamic_{}.time,
    dynamic_{}.longitude,
    dynamic_{}.latitude,
    dynamic_{}.sog,
    dynamic_{}.cog,
    static_{}.imo,
    static_{}.vessel_name,
    static_{}.dim_bow,
    static_{}.dim_stern,
    static_{}.dim_port,
    static_{}.dim_star,
    static_{}.ship_type,
    ref.coarse_type_txt AS ship_type_txt
  FROM dynamic_{}
  LEFT JOIN static_{} ON 
    dynamic_{}.mmsi = static_{}.mmsi
  LEFT JOIN ref ON 
    static_{}.ship_type = ref.coarse_type

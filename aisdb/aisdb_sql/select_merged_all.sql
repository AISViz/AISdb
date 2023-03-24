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
    ref.coarse_type_txt AS ship_type_txt,
    webdata_marinetraffic.name as vessel_name2,
    webdata_marinetraffic.vesseltype_generic,
    webdata_marinetraffic.vesseltype_detailed,
    webdata_marinetraffic.gross_tonnage,
    webdata_marinetraffic.summer_dwt, 
    webdata_marinetraffic.length_breadth, 
    webdata_marinetraffic.year_built
  FROM dynamic_{}
  LEFT JOIN static_{} ON 
    dynamic_{}.mmsi = static_{}.mmsi
  LEFT JOIN ref ON 
    static_{}.ship_type = ref.coarse_type
  LEFT JOIN webdata_marinetraffic ON
    dynamic_{}.mmsi = webdata_marinetraffic.mmsi

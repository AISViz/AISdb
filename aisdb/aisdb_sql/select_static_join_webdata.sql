SELECT
    d_{}.mmsi,
    --ais_{}_static.imo,
    --ais_{}_static.vessel_name,
    --coarsetype_ref.coarse_type_txt AS ship_type_txt,
    webdata_marinetraffic.name as vessel_name2,
    webdata_marinetraffic.vesseltype_generic,
    webdata_marinetraffic.vesseltype_detailed,
    webdata_marinetraffic.flag,
    webdata_marinetraffic.gross_tonnage,
    webdata_marinetraffic.summer_dwt, 
    webdata_marinetraffic.length_breadth, 
    webdata_marinetraffic.year_built
  FROM (
    SELECT DISTINCT mmsi FROM ais_{}_dynamic
  ) as d_{}
  LEFT JOIN webdata_marinetraffic ON
    d_{}.mmsi = webdata_marinetraffic.mmsi
  WHERE webdata_marinetraffic.error404 != 1

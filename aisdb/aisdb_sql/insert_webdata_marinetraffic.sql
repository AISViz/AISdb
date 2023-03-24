INSERT INTO webdata_marinetraffic (
    mmsi, 
    imo, 
    name,
    vesseltype_generic,
    vesseltype_detailed,
    callsign,
    flag,
    gross_tonnage,
    summer_dwt,
    length_breadth,
    year_built,
    home_port
  )
VALUES (CAST($1 AS INT),CAST($2 AS INT),$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (mmsi) DO UPDATE SET 
    imo = excluded.imo,
    name = excluded.name, 
    vesseltype_generic = excluded.vesseltype_generic,
    vesseltype_detailed = excluded.vesseltype_detailed,
    callsign = excluded.callsign,
    flag = excluded.flag,
    gross_tonnage = excluded.gross_tonnage,
    summer_dwt = excluded.gross_tonnage,
    length_breadth = excluded.length_breadth,
    year_built = excluded.year_built,
    home_port = excluded.home_port;

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
VALUES (CAST(? AS INT),CAST(? AS INT),?,?,?,?,?,?,?,?,?,?)
ON CONFLICT (mmsi, imo) DO UPDATE SET name = excluded.name;

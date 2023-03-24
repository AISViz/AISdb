CREATE TABLE IF NOT EXISTS webdata_marinetraffic (
    mmsi INTEGER PRIMARY KEY,
    imo INTEGER,
    name TEXT,
    vesseltype_generic TEXT,
    vesseltype_detailed TEXT,
    callsign TEXT,
    flag TEXT,
    gross_tonnage INTEGER,
    summer_dwt INTEGER,
    length_breadth TEXT,
    year_built INTEGER,
    home_port TEXT,
    error404 INTEGER NOT NULL DEFAULT 0
);

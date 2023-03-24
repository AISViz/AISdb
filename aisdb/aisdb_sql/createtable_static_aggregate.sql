CREATE TABLE static_{}_aggregate (
    mmsi INTEGER PRIMARY KEY,
    imo INTEGER,
    vessel_name TEXT,
    ship_type INTEGER,
    call_sign TEXT,
    dim_bow INTEGER,
    dim_stern INTEGER,
    dim_port INTEGER,
    dim_star INTEGER,
    draught INTEGER
);

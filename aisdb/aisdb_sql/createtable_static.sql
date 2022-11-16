CREATE TABLE IF NOT EXISTS ais_{}_static (
    mmsi INTEGER NOT NULL,
    time INTEGER NOT NULL,
    vessel_name TEXT,
    ship_type INTEGER,
    call_sign TEXT,
    imo INTEGER NOT NULL DEFAULT 0,
    dim_bow INTEGER,
    dim_stern INTEGER,
    dim_port INTEGER,
    dim_star INTEGER,
    draught INTEGER,
    destination TEXT,
    ais_version TEXT,
    fixing_device TEXT,
    eta_month INTEGER,
    eta_day INTEGER,
    eta_hour INTEGER,
    eta_minute INTEGER,
    source TEXT NOT NULL,
    PRIMARY KEY (mmsi, time, imo, source)
) WITHOUT ROWID;
--, STRICT

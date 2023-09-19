CREATE TABLE IF NOT EXISTS ais_{}_dynamic (
    mmsi INTEGER NOT NULL,
    time INTEGER NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    rot REAL,
    sog REAL,
    cog REAL,
    heading REAL,
    maneuver BOOLEAN,
    utc_second INTEGER,
    source TEXT NOT NULL
    --,
    --PRIMARY KEY (mmsi, time, longitude, latitude, sog, cog, source)
);

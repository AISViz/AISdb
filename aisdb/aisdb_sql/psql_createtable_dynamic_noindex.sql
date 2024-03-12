CREATE TABLE IF NOT EXISTS ais_{0}_dynamic (
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
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ais_{0}_dynamic_pkkey ON ais_{0}_dynamic (mmsi, time, longitude, latitude);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_longitude ON ais_{0}_dynamic (longitude);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_latitude ON ais_{0}_dynamic (latitude);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_time ON ais_{0}_dynamic (time);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_mmsi ON ais_{0}_dynamic (mmsi);

CREATE OR REPLACE TRIGGER before_insert_dynamic BEFORE INSERT ON
       ais_{0}_dynamic FOR EACH ROW EXECUTE FUNCTION dynamic_insert();
CREATE TABLE IF NOT EXISTS ais_{0}_dynamic
(
    mmsi          INTEGER NOT NULL,
    time          INTEGER NOT NULL,
    longitude     REAL NOT NULL,
    latitude      REAL NOT NULL,
    rot           REAL,
    sog           REAL,
    cog           REAL,
    heading       REAL,
    maneuver      BOOLEAN,
    utc_second    INTEGER,
    source        TEXT NOT NULL,
    geom          GEOMETRY(POINT, 4326)
                  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)) STORED,
    PRIMARY KEY (mmsi, time, latitude, longitude)
);

SELECT create_hypertable(
    'ais_{0}_dynamic',
    'time',
    partitioning_column => 'mmsi',
    number_partitions => 4,
    chunk_time_interval => 604800,
    if_not_exists => TRUE
);

ALTER TABLE ais_{0}_dynamic SET (
    timescaledb.compress = false,
    timescaledb.compress_orderby = 'time ASC, latitude ASC, longitude ASC',
    timescaledb.compress_segmentby = 'mmsi'
);

CREATE INDEX idx_ais_{0}_dynamic_geom ON ais_{0}_dynamic USING GIST (geom);
CREATE INDEX idx_ais_{0}_dynamic_time ON ais_{0}_dynamic USING BRIN (time);
CREATE TABLE IF NOT EXISTS ais_global_dynamic
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
    geom          GEOMETRY(POINT, 4326),
    PRIMARY KEY (mmsi, time, latitude, longitude)
);

SELECT create_hypertable(
    'ais_global_dynamic',
    'time',
    partitioning_column => 'mmsi',
    number_partitions => 4,
    chunk_time_interval => 604800
);

ALTER TABLE ais_global_dynamic SET (
    timescaledb.compress = false,
    timescaledb.compress_orderby = 'time ASC, latitude ASC, longitude ASC',
    timescaledb.compress_segmentby = 'mmsi'
);

CREATE INDEX idx_ais_global_dynamic_geom ON ais_global_dynamic USING GIST (geom);
CREATE INDEX idx_ais_global_dynamic_time ON ais_global_dynamic (time);
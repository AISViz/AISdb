CREATE TABLE ais_${0}_dynamic
(
    mmsi          INTEGER NOT NULL,
--     time          INTEGER NOT NULL CHECK (time >= ${start_ts} AND time < ${end_ts}),
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
    PRIMARY KEY (mmsi, time, latitude, longitude)
);

SELECT create_hypertable(
    'ais_${0}_dynamic',
    'time',
    partitioning_column => 'mmsi',
    number_partitions => 4,
    chunk_time_interval => 604800
);

ALTER TABLE ais_${0}_dynamic SET (
    timescaledb.compress = false,
    timescaledb.compress_orderby = 'time ASC, latitude ASC, longitude ASC',
    timescaledb.compress_segmentby = 'mmsi'
);
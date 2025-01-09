CREATE TABLE ais_${0}_static
(
    mmsi           INTEGER NOT NULL,
    time           INTEGER NOT NULL CHECK (time >= ${start_ts} AND time < ${end_ts}),
    vessel_name    TEXT,
    ship_type      INTEGER,
    call_sign      TEXT,
    imo            BIGINT DEFAULT 0 NOT NULL,
    dim_bow        INTEGER,
    dim_stern      INTEGER,
    dim_port       INTEGER,
    dim_star       INTEGER,
    draught        INTEGER,
    destination    TEXT,
    ais_version    INTEGER,
    fixing_device  TEXT,
    eta_month      INTEGER,
    eta_day        INTEGER,
    eta_hour       INTEGER,
    eta_minute     INTEGER,
    source         TEXT NOT NULL,
    PRIMARY KEY (mmsi, time)
);

SELECT create_hypertable(
        'ais_${0}_static',
        'time',
        partitioning_column => 'mmsi',
        number_partitions => 4,
        chunk_time_interval => 604800
);

ALTER TABLE ais_${0}_static SET (
    timescaledb.compress = false,
    timescaledb.compress_orderby = 'time ASC',
    timescaledb.compress_segmentby = 'mmsi'
);
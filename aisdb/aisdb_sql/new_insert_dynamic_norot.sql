INSERT INTO ais_global_dynamic
(
    mmsi,
    time,
    longitude,
    latitude,
    sog,
    cog,
    heading,
    maneuver,
    utc_second,
    source
)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (mmsi, time, latitude, longitude) DO NOTHING;

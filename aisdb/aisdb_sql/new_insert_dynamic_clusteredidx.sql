INSERT INTO ais_global_dynamic
(
    mmsi,
    time,
    longitude,
    latitude,
    rot,
    sog,
    cog,
    heading,
    maneuver,
    utc_second,
    source,
    geom
)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,ST_SetSRID(ST_MakePoint($3::REAL, $4::REAL), 4326))
ON CONFLICT (mmsi, time, latitude, longitude) DO NOTHING;
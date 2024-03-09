INSERT INTO ais_{}_dynamic
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
    source
)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT DO NOTHING
;

INSERT OR IGNORE INTO ais_{}_dynamic
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
VALUES (?,?,?,?,?,?,?,?,?,?,?);

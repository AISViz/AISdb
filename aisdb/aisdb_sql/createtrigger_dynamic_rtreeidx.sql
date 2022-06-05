CREATE TRIGGER IF NOT EXISTS idx_rtree_{}_dynamic
AFTER INSERT ON ais_{}_dynamic
BEGIN
    INSERT INTO rtree_{}_dynamic(
        --id,
        mmsi0, mmsi1,
        t0, t1,
        x0, x1,
        y0, y1,
        rot,
        sog,
        cog,
        heading,
        utc_second,
        source
    )
    VALUES (
        --new.ROWID,
        new.mmsi, new.mmsi,
        new.time, new.time,
        new.longitude, new.longitude,
        new.latitude, new.latitude,
        new.rot,
        new.sog,
        new.cog,
        new.heading,
        new.utc_second,
        new.source
    )
; END

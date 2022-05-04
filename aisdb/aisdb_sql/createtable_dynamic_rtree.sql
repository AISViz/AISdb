CREATE VIRTUAL TABLE IF NOT EXISTS rtree_{}_dynamic USING rtree(
    id,
    mmsi0, mmsi1,
    t0, t1,
    x0, x1,
    y0, y1,
    +rot FLOAT,
    +sog FLOAT,
    +cog FLOAT,
    +heading FLOAT,
    +maneuver TEXT,
    +utc_second INT,
    +source TEXT
); 

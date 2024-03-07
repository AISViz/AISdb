CREATE OR REPLACE FUNCTION dynamic_insert()
RETURNS TRIGGER AS $$
DECLARE
    target_table TEXT;
BEGIN
    -- Validate the timestamp
    IF NEW.time IS NULL OR NEW.time < 0 THEN
        RAISE EXCEPTION 'Invalid timestamp value: %', NEW.time;
    END IF;

    -- Assuming NEW.time is the epoch timestamp in seconds
    target_table := lower('ais_' || TO_CHAR(TO_TIMESTAMP(NEW.time), 'YYYYMM') || '_dynamic');

    BEGIN
        -- Check if the target table exists, if not, create it
        IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = target_table) THEN
            EXECUTE 'CREATE TABLE ' || target_table || ' (LIKE ' || TG_TABLE_NAME || ' INCLUDING ALL)';
        END IF;
        -- Insert the new row into the correct table
        EXECUTE 'INSERT INTO ' || target_table || ' VALUES (($1).*)' USING NEW;
    END;

    -- Skip the original insert operation
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS ais_{0}_dynamic (
    mmsi INTEGER NOT NULL,
    time INTEGER NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    rot REAL,
    sog REAL,
    cog REAL,
    heading REAL,
    maneuver BOOLEAN,
    utc_second INTEGER,
    source TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ais_{0}_dynamic_pkkey ON ais_{0}_dynamic (mmsi, time, longitude, latitude);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_longitude ON ais_{0}_dynamic (longitude);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_latitude ON ais_{0}_dynamic (latitude);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_time ON ais_{0}_dynamic (time);
CREATE INDEX IF NOT EXISTS idx_{0}_dynamic_mmsi ON ais_{0}_dynamic (mmsi);

CREATE OR REPLACE TRIGGER before_insert_dynamic BEFORE INSERT ON
       ais_{0}_dynamic FOR EACH ROW EXECUTE FUNCTION dynamic_insert();
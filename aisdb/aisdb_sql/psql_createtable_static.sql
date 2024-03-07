CREATE OR REPLACE FUNCTION static_insert()
RETURNS TRIGGER AS $$
DECLARE
    target_table TEXT;
BEGIN
    -- Validate the timestamp
    IF NEW.time IS NULL OR NEW.time < 0 THEN
        RAISE EXCEPTION 'Invalid timestamp value: %', NEW.time;
    END IF;

    -- Assuming NEW.time is the epoch timestamp in seconds
    target_table := lower('ais_' || TO_CHAR(TO_TIMESTAMP(NEW.time), 'YYYYMM') || '_static');

    BEGIN
        -- Check if the target table exists, if not, create it
        IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = lower(target_table)) THEN
            EXECUTE 'CREATE TABLE ' || target_table || ' (LIKE ' || TG_TABLE_NAME || ' INCLUDING ALL)';
        END IF;
        -- Insert the new row into the correct table
        EXECUTE 'INSERT INTO ' || target_table || ' VALUES (($1).*)' USING NEW;
    END;

    -- Skip the original insert operation
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS ais_{0}_static (
    mmsi INTEGER NOT NULL,
    time INTEGER NOT NULL,
    vessel_name TEXT,
    ship_type INTEGER,
    call_sign TEXT,
    imo INTEGER NOT NULL DEFAULT 0,
    dim_bow INTEGER,
    dim_stern INTEGER,
    dim_port INTEGER,
    dim_star INTEGER,
    draught INTEGER,
    destination TEXT,
    ais_version INTEGER,
    fixing_device TEXT,
    eta_month INTEGER,
    eta_day INTEGER,
    eta_hour INTEGER,
    eta_minute INTEGER,
    source TEXT NOT NULL,
    PRIMARY KEY (mmsi, time, imo, source)
);

CREATE OR REPLACE TRIGGER before_insert_static BEFORE INSERT ON
       ais_{0}_static FOR EACH ROW EXECUTE FUNCTION static_insert();
CREATE OR REPLACE FUNCTION dynamic_insert()
RETURNS TRIGGER AS $$
DECLARE
    target_table TEXT;
BEGIN
    -- Assuming NEW.time is the epoch timestamp in seconds
    target_table := lower('ais_' || TO_CHAR(TO_TIMESTAMP(NEW.time), 'YYYYMM') || '_dynamic');

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
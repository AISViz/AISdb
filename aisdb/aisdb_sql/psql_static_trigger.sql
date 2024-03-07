CREATE OR REPLACE FUNCTION static_insert()
RETURNS TRIGGER AS $$
DECLARE
    target_table TEXT;
BEGIN
    -- Assuming NEW.time is the epoch timestamp in seconds
    target_table := lower('ais_' || TO_CHAR(TO_TIMESTAMP(NEW.time), 'YYYYMM') || '_static');

    -- Check if the target table exists, if not, create it
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = target_table) THEN
        EXECUTE 'CREATE TABLE ' || quote_ident(target_table) || ' (LIKE ' || quote_ident(TG_TABLE_NAME) || ' INCLUDING ALL)';
    END IF;

    -- Prepare and execute the dynamic INSERT statement
    EXECUTE 'INSERT INTO ' || quote_ident(target_table) || ' SELECT * FROM (VALUES (''' || NEW || ''')) AS t';

    -- Skip the original INSERT operation
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
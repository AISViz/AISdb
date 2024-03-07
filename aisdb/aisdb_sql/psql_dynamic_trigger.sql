CREATE OR REPLACE FUNCTION dynamic_insert()
RETURNS TRIGGER AS $$
DECLARE
    target_table TEXT;
    cols TEXT;
    vals TEXT;
BEGIN
    -- Assuming NEW.time is the epoch timestamp in seconds
    target_table := lower('ais_' || TO_CHAR(TO_TIMESTAMP(NEW.time), 'YYYYMM') || '_dynamic');

    -- Creating the table if it does not exist
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = target_table) THEN
        EXECUTE format('CREATE TABLE public.%I (LIKE public.%I INCLUDING ALL)', target_table, TG_TABLE_NAME);
    END IF;

    -- Extracting columns and values from the NEW row as text
    SELECT string_agg(quote_ident(key), ', ') INTO cols FROM each(hstore(NEW));
    SELECT string_agg(quote_literal(value), ', ') INTO vals FROM each(hstore(NEW));

    -- Dynamically building and executing the INSERT statement
    EXECUTE format('INSERT INTO public.%I (%s) VALUES (%s)', target_table, cols, vals);

    -- Skip the original INSERT operation
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
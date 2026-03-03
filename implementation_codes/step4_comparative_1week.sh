#!/bin/bash
# Benchmark B-Tree vs BRIN (1 week - robust version)
# Same approach as 1 day, just loading 7 days

export PGPASSWORD="${PGPASSWORD:-your_password_here}"
CSV_PATH="${CSV_PATH:-/path/to/your/data/202010}"

echo "=========================================="
echo "BENCHMARK: B-TREE vs BRIN (1 WEEK - ROBUST)"
echo "=========================================="
echo ""

# ============================================
# PART 1: B-TREE SETUP
# ============================================
echo "PART 1: B-Tree Setup"
echo "===================="

DB_BTREE="aisdb_btree_1week_robust"
PGHOST="${PGHOST:-localhost}"
PGUSER="${PGUSER:-postgres}"

psql -h $PGHOST -U $PGUSER -d postgres -c "DROP DATABASE IF EXISTS $DB_BTREE;" 2>/dev/null
psql -h $PGHOST -U $PGUSER -d postgres -c "CREATE DATABASE $DB_BTREE;" 2>/dev/null

psql -h $PGHOST -U $PGUSER -d $DB_BTREE << 'SQL' >/dev/null 2>&1
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE ais_global_dynamic (
    mmsi INT,
    time BIGINT NOT NULL,
    longitude NUMERIC,
    latitude NUMERIC,
    sog NUMERIC,
    cog NUMERIC,
    heading INT,
    status INT,
    destination TEXT,
    callsign TEXT,
    vesselname TEXT
);

SELECT create_hypertable('ais_global_dynamic', 'time', if_not_exists => TRUE);
CREATE INDEX idx_ais_global_dynamic_btree_time ON ais_global_dynamic USING BTREE (time);
SQL

echo "✓ B-Tree database and structure created"

# ============================================
# PART 2: BRIN SETUP
# ============================================
echo ""
echo "PART 2: BRIN Setup"
echo "=================="

DB_BRIN="aisdb_brin_1week_robust"
psql -h $PGHOST -U $PGUSER -d postgres -c "DROP DATABASE IF EXISTS $DB_BRIN;" 2>/dev/null
psql -h $PGHOST -U $PGUSER -d postgres -c "CREATE DATABASE $DB_BRIN;" 2>/dev/null

psql -h $PGHOST -U $PGUSER -d $DB_BRIN << 'SQL' >/dev/null 2>&1
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE ais_global_dynamic (
    mmsi INT,
    time BIGINT NOT NULL,
    longitude NUMERIC,
    latitude NUMERIC,
    sog NUMERIC,
    cog NUMERIC,
    heading INT,
    status INT,
    destination TEXT,
    callsign TEXT,
    vesselname TEXT
);

SELECT create_hypertable('ais_global_dynamic', 'time', if_not_exists => TRUE);
CREATE INDEX idx_ais_global_dynamic_brin_time ON ais_global_dynamic USING BRIN (time);
SQL

echo "✓ BRIN database and structure created"

# ============================================
# PART 3: LOAD DATA (7 DAYS, WITH VALIDATION)
# ============================================
echo ""
echo "PART 3: Loading data (7 days, with validation)"
echo "================================================"

echo "Processing and validating CSVs (Oct 01-07)..."

total_btree_time=0
total_brin_time=0

for day in {01..07}; do
    CSV_FILE="$CSV_PATH/exactEarth_historical_data_2020-10-${day}.csv"
    if [ ! -f "$CSV_FILE" ]; then
        echo "✗ File not found: $CSV_FILE"
        continue
    fi

    echo "Day $day: processing $CSV_FILE"

    # Criar arquivo temporário limpo
    TMP_CSV="/tmp/ais_clean_${day}_$(date +%s).csv"

    # Processar CSV com validação: pular linhas com dados inválidos
    tail -n +2 "$CSV_FILE" | tr -d '"' | awk -F',' '{
        # Main fields
        mmsi=$1
        time=$4
        milli=$5
        lon=$26
        lat=$27
        sog=$19
        cog=$21
        heading=$22
        status=$20
        dest=$24
        call=$15
        vessel=$14
        
        # Clean spaces
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", mmsi)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", time)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", milli)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", lon)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", lat)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", sog)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", cog)
        
        # Validation: lon and lat MUST be numbers (not strings like "USA")
        if (lon !~ /^-?[0-9.]*$/ || lat !~ /^-?[0-9.]*$/) {
            next
        }
        
        # Convert None to empty
        if (lon == "None" || lon == "") lon = ""
        if (lat == "None" || lat == "") lat = ""
        if (sog == "None" || sog == "") sog = ""
        if (cog == "None" || cog == "") cog = ""
        if (heading == "None" || heading == "") heading = ""
        if (status == "None" || status == "") status = ""
        if (dest == "None" || dest == "") dest = ""
        if (call == "None" || call == "") call = ""
        if (vessel == "None" || vessel == "") vessel = ""
        
        # Validate timestamp
        fulltime = time milli
        gsub(/[^0-9]/, "", fulltime)
        if (fulltime == "") next
        if (fulltime ~ /^0+$/) next  # skip zero timestamps
        
        # Output
        print mmsi "|" lon "|" lat "|" sog "|" cog "|" heading "|" status "|" dest "|" call "|" vessel "|" fulltime
    }' > "$TMP_CSV"

    TOTAL_LINES=$(wc -l < "$TMP_CSV")
    echo "✓ Clean file generated: $TOTAL_LINES valid lines"

    echo "Loading into B-Tree..."
    START=$(date +%s)
    psql -h $PGHOST -U $PGUSER -d $DB_BTREE -c "\copy ais_global_dynamic (mmsi, longitude, latitude, sog, cog, heading, status, destination, callsign, vesselname, time) FROM '$TMP_CSV' WITH (FORMAT csv, DELIMITER '|', NULL '')" 2>&1 | grep -i "copy"
    END=$(date +%s)
    BTREE_TIME=$((END - START))
    total_btree_time=$((total_btree_time + BTREE_TIME))
    echo "✓ B-Tree: ${BTREE_TIME}s"

    echo "Loading into BRIN..."
    START=$(date +%s)
    psql -h $PGHOST -U $PGUSER -d $DB_BRIN -c "\copy ais_global_dynamic (mmsi, longitude, latitude, sog, cog, heading, status, destination, callsign, vesselname, time) FROM '$TMP_CSV' WITH (FORMAT csv, DELIMITER '|', NULL '')" 2>&1 | grep -i "copy"
    END=$(date +%s)
    BRIN_TIME=$((END - START))
    total_brin_time=$((total_brin_time + BRIN_TIME))
    echo "✓ BRIN: ${BRIN_TIME}s"

    rm "$TMP_CSV"
    echo ""
done

echo "Total load time: B-Tree=${total_btree_time}s | BRIN=${total_brin_time}s"

# ============================================
# PART 4: VERIFY DATA
# ============================================
echo ""
echo "PART 4: Record Count"
echo "===================="

echo "B-Tree:"
psql -h $PGHOST -U $PGUSER -d $DB_BTREE -c "SELECT COUNT(*) as total_registros FROM ais_global_dynamic;" 2>/dev/null

echo ""
echo "BRIN:"
psql -h $PGHOST -U $PGUSER -d $DB_BRIN -c "SELECT COUNT(*) as total_registros FROM ais_global_dynamic;" 2>/dev/null

# ============================================
# PART 5: SIZES
# ============================================
echo ""
echo "PART 5: Size Analysis"
echo "====================="

echo ""
echo "B-Tree:"
psql -h $PGHOST -U $PGUSER -d $DB_BTREE << 'SQL' 2>/dev/null
WITH chunk_sizes AS (
    SELECT 
        SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total,
        SUM(pg_relation_size(schemaname||'.'||tablename)) as data_only
    FROM pg_tables 
    WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper%chunk'
)
SELECT 
    'Data (without indexes)' as type,
    pg_size_pretty((SELECT data_only FROM chunk_sizes)) as size
UNION ALL
SELECT 
    'Indexes (total)',
    pg_size_pretty((SELECT total - data_only FROM chunk_sizes))
UNION ALL
SELECT 
    'Total (data+indexes)',
    pg_size_pretty((SELECT total FROM chunk_sizes));
SQL

echo ""
echo "BRIN:"
psql -h $PGHOST -U $PGUSER -d $DB_BRIN << 'SQL' 2>/dev/null
WITH chunk_sizes AS (
    SELECT 
        SUM(pg_total_relation_size(schemaname||'.'||tablename)) as total,
        SUM(pg_relation_size(schemaname||'.'||tablename)) as data_only
    FROM pg_tables 
    WHERE schemaname = '_timescaledb_internal' AND tablename LIKE '_hyper%chunk'
)
SELECT 
    'Data (without indexes)' as type,
    pg_size_pretty((SELECT data_only FROM chunk_sizes)) as size
UNION ALL
SELECT 
    'Indexes (total)',
    pg_size_pretty((SELECT total - data_only FROM chunk_sizes))
UNION ALL
SELECT 
    'Total (data+indexes)',
    pg_size_pretty((SELECT total FROM chunk_sizes));
SQL

# ============================================
# PART 6: QUERY BENCHMARK
# ============================================
echo ""
echo "PART 6: Query Benchmark"
echo "======================="

echo ""
echo "Query 1: 1 hour range"
echo "B-Tree:"
psql -h $PGHOST -U $PGUSER -d $DB_BTREE 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001130000000;
SQL

echo "BRIN:"
psql -h $PGHOST -U $PGUSER -d $DB_BRIN 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001130000000;
SQL

echo ""
echo "Query 2: 6 hours range"
echo "B-Tree:"
psql -h $PGHOST -U $PGUSER -d $DB_BTREE 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001180000000;
SQL

echo "BRIN:"
psql -h $PGHOST -U $PGUSER -d $DB_BRIN 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001180000000;
SQL

echo ""
echo "Query 3: 24 hours range"
echo "B-Tree:"
psql -h $PGHOST -U $PGUSER -d $DB_BTREE 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201002000000000;
SQL

echo "BRIN:"
psql -h $PGHOST -U $PGUSER -d $DB_BRIN 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201002000000000;
SQL

echo ""
echo "Query 4: 7 days range"
echo "B-Tree:"
psql -h $PGHOST -U $PGUSER -d $DB_BTREE 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201008000000000;
SQL

echo "BRIN:"
psql -h $PGHOST -U $PGUSER -d $DB_BRIN 2>/dev/null << 'SQL' | grep "Execution Time"
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201008000000000;
SQL

echo ""
echo "=========================================="
echo "✓ BENCHMARK COMPLETE!"
echo "=========================================="

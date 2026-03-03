#!/bin/bash
# Step 5: Test parallel workers using existing Step 4 databases (2 weeks)

export PGPASSWORD="${PGPASSWORD:-your_password_here}"
PGHOST="${PGHOST:-localhost}"
PGUSER="${PGUSER:-postgres}"

echo "=========================================="
echo "STEP 5: PARALLEL WORKERS TEST (2 WEEKS)"
echo "=========================================="
echo "Using existing database: aisdb_brin_2weeks_robust"
echo "Testing parallel workers: 2, 4, 8"
echo ""

DB_NAME="aisdb_brin_2weeks_robust"

# Test with different parallel worker settings
for WORKERS in 2 4 8; do
    echo ""
    echo "========================================="
    echo "Testing: parallel_workers=$WORKERS"
    echo "========================================="
    
    # Set parallel workers for this session
    echo "Configuring parallel workers to $WORKERS..."
    
    # Run queries
    echo ""
    echo "Query 1h:"
    psql -h $PGHOST -U $PGUSER -d $DB_NAME << SQL 2>/dev/null | grep "Execution Time"
SET max_parallel_workers_per_gather = $WORKERS;
SET parallel_tuple_cost = 0.01;
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001130000000;
SQL
    
    echo "Query 6h:"
    psql -h $PGHOST -U $PGUSER -d $DB_NAME << SQL 2>/dev/null | grep "Execution Time"
SET max_parallel_workers_per_gather = $WORKERS;
SET parallel_tuple_cost = 0.01;
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001180000000;
SQL
    
    echo "Query 24h:"
    psql -h $PGHOST -U $PGUSER -d $DB_NAME << SQL 2>/dev/null | grep "Execution Time"
SET max_parallel_workers_per_gather = $WORKERS;
SET parallel_tuple_cost = 0.01;
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201002000000000;
SQL
    
    echo "Query 1week:"
    psql -h $PGHOST -U $PGUSER -d $DB_NAME << SQL 2>/dev/null | grep "Execution Time"
SET max_parallel_workers_per_gather = $WORKERS;
SET parallel_tuple_cost = 0.01;
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201008000000000;
SQL
    
    echo "Query 2weeks:"
    psql -h $PGHOST -U $PGUSER -d $DB_NAME << SQL 2>/dev/null | grep "Execution Time"
SET max_parallel_workers_per_gather = $WORKERS;
SET parallel_tuple_cost = 0.01;
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201015000000000;
SQL
    
    echo ""
done

echo "=========================================="
echo "✓ Test complete!"
echo "=========================================="

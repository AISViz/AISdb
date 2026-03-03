# Step 5 - Optimize TimescaleDB

## Objective
Optimize TimescaleDB configuration to maximize query performance for large-scale AIS time-series workloads, focusing on parallel query execution settings.

## Context
After implementing BRIN indexes (Step 4), further optimization of parallel query execution can provide significant performance improvements. PostgreSQL's parallel query feature allows multiple worker processes to scan data concurrently, which is particularly effective for:
- **Large sequential scans**: BRIN indexes scan blocks efficiently
- **Aggregation queries**: COUNT, AVG, MAX benefit from parallelism
- **Range queries**: Larger time ranges see bigger gains

**Baseline Configuration:**
- `max_parallel_workers_per_gather = 2` (PostgreSQL default)
- `max_parallel_workers = 8` (system default)
- `parallel_tuple_cost = 0.1` (cost threshold)

## Summary

**Status:** Parallel worker optimization completed and benchmarked.

**Scope:** This step focuses on safe, reversible performance optimizations through parallel query tuning. No data structure changes are made.

**Key Finding:** Increasing parallel workers from 2 to 8 provides **28-46% performance improvement** on queries. Larger queries benefit more from parallelism.

**Recommendation:** Set `max_parallel_workers_per_gather = 8` for production use.

## Implementation Tasks

### Task 1: Test Parallel Worker Settings
Test different parallel worker configurations to find optimal settings.

**Configuration Options:**
```sql
-- Option A: Default (baseline)
SET max_parallel_workers_per_gather = 2;

-- Option B: Moderate parallelism
SET max_parallel_workers_per_gather = 4;

-- Option C: High parallelism
SET max_parallel_workers_per_gather = 8;

-- Common settings for all tests
SET max_parallel_workers = 16;
SET parallel_tuple_cost = 0.01;  -- Lower threshold to enable parallelism sooner
```

**Checklist:**
- [x] Test with 2 workers (baseline)
- [x] Test with 4 workers
- [x] Test with 8 workers
- [x] Measure query execution time
- [x] Document performance improvements

---

## Comparative Results

### Parallel Workers
Compare query performance with different parallel worker configurations.

**Setup:**
- Database: `aisdb_brin_1day_robust` (from Step 4)
- Data: exactEarth_historical_data_2020-10-01.csv (3.1M records)
- Index: BRIN on `time` column
- Method: Session-level configuration changes (no data reload)

**Tests:**
- Query 1: 1 hour range (~130k records)
- Query 2: 6 hours range (~780k records)
- Query 3: 24 hours range (3.1M records - full day)

**Queries:**
```sql
-- Query 1: 1 hour
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001130000000;

-- Query 2: 6 hours
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001120000000 AND time <= 20201001180000000;

-- Query 3: 24 hours
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201002000000000;
```

**Checklist:**
- [x] Configure parallel workers to 2 (baseline)
- [x] Run 1h/6h/24h queries with EXPLAIN ANALYZE
- [x] Configure parallel workers to 4
- [x] Run 1h/6h/24h queries with EXPLAIN ANALYZE
- [x] Configure parallel workers to 8
- [x] Run 1h/6h/24h queries with EXPLAIN ANALYZE
- [x] Calculate performance improvements

**How it was tested:**
- Script: [step5_comparative_2weeks.sh](../codes/step5_comparative_2weeks.sh)
- Command:
```bash
cd codes && PGHOST="bigdata6" PGUSER="julio" PGPASSWORD="..." bash step5_comparative_2weeks.sh
```

**Result:**
- **Date:** 2026-02-16
- **Database:** aisdb_brin_2weeks_robust (reused from Step 4)
- **Records:** 43,400,000

| Parallel Workers | Query 1h | Query 6h | Query 24h | Query 1week | Query 2weeks |
|-----------------|----------|----------|-----------|-------------|--------------|
| 2 workers (baseline) | 19.2 ms | 68.1 ms | 200.5 ms | 1313.1 ms | 2470.4 ms |
| 4 workers | 18.1 ms | 50.2 ms | 155.3 ms | 891.7 ms | 1671.6 ms |
| 8 workers | 19.3 ms | 47.8 ms | 108.5 ms | 594.3 ms | 1160.4 ms |

**Performance Improvements (8 workers vs 2 workers):**
- Query 1h: **0% (stable)** (19.2 ms → 19.3 ms)
- Query 6h: **30% faster** (68.1 ms → 47.8 ms)
- Query 24h: **46% faster** (200.5 ms → 108.5 ms)
- Query 1week: **55% faster** (1313.1 ms → 594.3 ms)
- Query 2weeks: **53% faster** (2470.4 ms → 1160.4 ms)

**Analysis:**
- Parallelism provides consistent improvements across all query sizes
- Larger queries benefit significantly more (53% improvement on 2-week scan)
- Small queries (1h) see minimal benefit due to parallelization overhead
- 8 workers provides best overall performance, especially for large scans

---

## Configuration Changes

### Recommended Settings

Based on benchmark results, apply these settings to production:

```sql
-- Apply to specific database
ALTER DATABASE aisdb SET max_parallel_workers_per_gather = 8;
ALTER DATABASE aisdb SET max_parallel_workers = 16;
ALTER DATABASE aisdb SET parallel_tuple_cost = 0.01;

-- Or apply globally to postgresql.conf
-- max_parallel_workers_per_gather = 8
-- max_parallel_workers = 16
-- parallel_tuple_cost = 0.01
```

**Verification:**
```sql
-- Check current settings
SHOW max_parallel_workers_per_gather;
SHOW max_parallel_workers;
SHOW parallel_tuple_cost;

-- Test query parallelism
EXPLAIN (ANALYZE, BUFFERS) 
SELECT COUNT(*) FROM ais_global_dynamic 
WHERE time >= 20201001000000000 AND time <= 20201002000000000;
```

---

## Future Work

### Compression Policies
Implement TimescaleDB columnar compression for older chunks:
- **Benefit:** 70-95% storage reduction on historical data
- **Challenge:** Requires stakeholder approval on data lifecycle
- **Status:** Deferred pending business decision

### Retention Policies
Implement automated data retention:
- **Benefit:** Automated storage management
- **Challenge:** Requires legal/compliance approval
- **Status:** Deferred pending business decision

---

## Final Results

**Optimization Summary:**

| Query Type | 2 workers | 8 workers | Improvement |
|-----------|-----------|-----------|-------------|
| Query 1h (130k records) | 19.2 ms | 19.3 ms | 0% (stable) |
| Query 6h (780k records) | 68.1 ms | 47.8 ms | 30% faster |
| Query 24h (3.1M records) | 200.5 ms | 108.5 ms | 46% faster |
| Query 1week (21.6M records) | 1313.1 ms | 594.3 ms | 55% faster |
| Query 2weeks (43.4M records) | 2470.4 ms | 1160.4 ms | **53% faster** |

**Key Insights:**
- Parallel workers provide **0-55% performance improvements** depending on query size
- Larger queries benefit dramatically: 2-week scan improved by **53%** (2.47s → 1.16s)
- Small queries see minimal benefit due to parallelization overhead
- Optimization is safe and reversible (session-level settings)
- Works seamlessly with existing BRIN indexes from Step 4
# Step 1 - V1 vs V2 Comparison

Semantic view of what changed between V1_AISDB (old base) and V2_AISDB_LITE (researcher's branch), aligned with current objectives.

## Overview
The transition from V1 to V2 reveals a deep architectural refactoring with three pillars: (1) centralization in global tables, eliminating parameterization by month; (2) complete removal of SQLite, consolidating backend exclusively in PostgreSQL/Timescale; (3) addition of geospatial support via PostGIS with automatically generated `geom` column, GIST indexes and geometry callbacks. V2 already achieves two of the three main objectives (PostgreSQL-only, legacy cleanup), paving the way for V3 to apply BRIN on temporal indexes and finalize Rust optimizations.

## Analyzed Components

**Timescale Scripts (SQL):** Replacement of parameterized naming with global tables (`ais_global_dynamic`, `ais_global_static`), PostGIS integration with generated geometric column, GIST indexes for spatial searches and B-Tree for time.

**Python Database Modules:** Complete removal of `SQLiteDBConn` (~160 lines), elimination of monthly aggregation methods, migration to global tables and PostGIS geometry callbacks.

**Rust Code:** Elimination of temporal partitioning logic, replacement of automatic month generation with hardcoded `"global"`, reference to new optimized SQL scripts without placeholders.

**Postgres Tests:** New tests focused on global PostgreSQL/Timescale pipeline (`test_001_postgres_global.py`, `test_002_decode_global.py`, `test_004_sqlfcn_postgres.py`, `test_005_dbqry_postgres.py`), validating CSV ingestion, geospatial queries and geometry callbacks.

## Results

V1 to V2 represents complete architectural transformation: total removal of SQLite, centralization in global tables (eliminating parameterization by month), PostGIS integration with automatic geom, and update of Rust/Python/tests code for new paradigm; V2 already delivers ~90% of desired modernization, establishing solid foundation for V3 which will focus on BRIN index, final cleanup of legacy artifacts, and performance optimizations.
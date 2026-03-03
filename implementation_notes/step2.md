# Step 2 - Define detailed technical plan for V3

## Objective
Consolidate decisions on what will be maintained, removed and added when migrating from V2 to V3.

## Decision Matrix

### Maintain (from V2 to V3)
- **PostgreSQL/Timescale as single backend** — V2 already completely removed SQLite (~160 lines), exclusive focus on `PostgresDBConn`
- **Centralized global tables** — `ais_global_dynamic`, `ais_global_static` eliminate parameterization by month, simplify queries and maintenance
- **Optimized SQL scripts** — `new_insert_static.sql`, `new_insert_dynamic_clusteredidx.sql`, global aggregations, clustered index CTEs
- **PostGIS integration** — `geom` column generated automatically, GIST indexes for spatial searches, geometry callbacks
- **Postgres/global tests** — `test_001_postgres_global.py`, `test_002_decode_global.py`, `test_004_sqlfcn_postgres.py`, `test_005_dbqry_postgres.py`
- **Modernized Python layer** — `dbconn.py`, `dbqry.py`, `create_tables.py` focused on Postgres/Timescale, without SQLite dependencies
- **Aligned Rust** — `db.rs` with global tables logic, optimized scripts, without temporal partitioning

### Remove (definitively in V3)
- **SQLite** — Already removed in V2 (`SQLiteDBConn` class, monthly aggregation methods, imports)
- **Visualization module** — `aisdb/ports` (absent in V2), remove residues if they exist
- **Ports notebooks** — `examples/get_ports.ipynb` (absent in V2), validate complete removal
- **Obsolete tests** — V2 already removed `test_003_createtables.py`, `test_009_wsa.py`, `test_010_network_graph.py`; check if `test_011_ui.py` has UI residues
- **Legacy artifacts** — Validate non-existence of SQLite configs, orphan imports, old parameterized SQL scripts

### Add (new implementations for V3)
- **BRIN index on temporal columns** — Replace B-Tree with BRIN on `time` (already validated: 625x smaller, 3.3x faster in range queries). Change `timescale_createtable_dynamic.sql`: `CREATE INDEX ... USING BRIN (time)` instead of B-Tree.
- **Final structural cleanup** — Sweep codebase for residual SQLite imports/configs, remove legacy code comments
- **PostgreSQL-only documentation** — Update README, docstrings, examples reflecting single backend
- **BRIN performance tests** — Add comparative benchmark BRIN vs B-Tree in Timescale environment (similar to performed in `tests/brin_test.py`)
- **Rust optimizations** — Evaluate if `db.rs` can have bulk insert performance improvements (already uses optimized scripts, but check async/parallelism)

## Results

V2 already delivers ~90% of objectives (PostgreSQL-only, global tables, PostGIS, modern tests, aligned Python/Rust), forming solid foundation for V3; complete cleanup validation (UI/ports already absent, check residues in tests/configs) needs to be finalized; main focus in V3 will be **BRIN index** (proven gain of 625x space, 3.3x speed), final structural cleanup, documentation and validation tests, with estimate of 2-3 implementation iterations.
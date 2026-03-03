# Project Summary (ais_lightning)

## Context
This project will continue previous work to modernize the AIS system, focusing only on the backend. The goal is to deliver a clean and optimized version, removing unused modules and technologies and concentrating efforts on PostgreSQL and performance improvements.

## Folder Structure
- **v1**: original version (visualization module, legacy Python, SQLite, etc.)
- **v2**: branch from a previous researcher (incomplete, but aligned with objectives)
- **v3**: copy of v2 where final changes will be made (v2 will remain as comparison baseline)

## Main Objectives
1. **Repository cleanup**
   - Remove unused modules (visualization, etc).
   - Remove SQLite support.
   - Keep only PostgreSQL as backend.

2. **Implementation of priority improvements**
   - **Rust migration** (performance-critical components).
   - **TimescaleDB integration** (partitioning and temporal optimization).
   - **BRIN Index** for temporal columns in large tables.

## Expected Deliverables
- Organized and simplified code (without obsolete modules).
- Functional version in PostgreSQL (without SQLite).
- Improvements implemented and documented in v3.
- Objective comparison between v2 and v3 (what changed and why).

## Project Steps
1. Review differences between v1 and v2 (architectural comparison)
2. Define detailed technical plan for v3 (decision matrix)
3. Execute repository cleanup in v3 (remove obsolete artifacts)
4. Implement BRIN Index (temporal index optimization)
5. Optimize TimescaleDB (configurations, compression, retention)
6. Rust migration (optimize performance-critical components)
7. Documentation and Finalization (final report, deliverables)
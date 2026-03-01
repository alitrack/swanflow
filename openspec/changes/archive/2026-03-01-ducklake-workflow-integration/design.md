## Context

The current `rustflow` engine handles intermediate state by writing DataFusion `RecordBatch` streams to Parquet files named by their node hash. This approach works but is "dumb"—it has no transactional awareness, no versioning, and requires explicit input mapping for nodes to find each other's data. `datafusion-ducklake` provides a metadata layer (powered by DuckDB) that tracks these Parquet files as tables, enabling a true Lakehouse architecture within the workflow.

## Goals / Non-Goals

**Goals:**
- **Centralized Catalog**: All materialized nodes must be registered in a DuckLake catalog automatically.
- **Transactional Writes**: Use DuckLake's snapshot mechanism to ensure data integrity.
- **SQL Table Mapping**: Enable `node_<uuid>` as a first-class table in the DataFusion context.
- **Storage Evolution**: Move cache management from raw files to a structured Lakehouse directory (`.ducklake/`).

**Non-Goals:**
- **Replacing DataFusion**: DataFusion remains the execution engine; DuckLake is the catalog and state manager.
- **Cloud Object Storage (Phase 1)**: Initial implementation will focus on local filesystem backends for simplicity.

## Decisions

### 1. DuckDB as the Metadata Backend
**Rationale**: Since `rustflow` already uses DuckDB for some operations and it's highly efficient for embedded OLAP, we will use it as the metadata storage for DuckLake.
- **Alternative**: SQLite. Rejected as DuckDB offers better integration with Arrow/DataFusion patterns.

### 2. Standardized Table Naming (`node_<uuid_short>`)
**Rationale**: To allow easy SQL referencing, we will map every materialized node to a table name.
- **Pattern**: `node_` + first 8 characters of the node UUID.
- **Registry**: The `Materializer` will handle registration during the execution phase.

### 3. "Live Snapshot" during Execution
**Rationale**: Every time a node finishes materializing, it will commit a new snapshot to the DuckLake catalog.
- **Advantage**: If a workflow fails mid-run, previous nodes' data remains consistent and accessible for inspection via the catalog.

## Risks / Trade-offs

- **[Risk] Metadata Lock Contention** → **Mitigation**: Use a single DuckDB connection for catalog metadata updates or manage a pool if parallel commits are frequent.
- **[Risk] Dependency Conflict** → **Mitigation**: `datafusion-ducklake` is a relatively new crate; we may need to contribute or fork if there are severe version mismatches with DF 52.1.0.

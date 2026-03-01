## Why

Currently, `rustflow` uses a basic Parquet-based caching mechanism that lacks centralized management, snapshot consistency, and cross-node SQL interoperability. By integrating `datafusion-ducklake`, we transform the workflow engine into a "Live Lakehouse," where every node's materialized output becomes a versioned table in a unified catalog. This enables ACID properties, millisecond-level interactive previews, and the ability to reference any node's output via standard SQL without explicit wiring.

## What Changes

- **Core Engine Upgrade**: Integrate `datafusion-ducklake` as the primary `CatalogProvider` in `SwanFlowEngine`.
- **Materializer Refactoring**: Re-implement `MaterializerLogical` and its physical executor to write data into DuckLake tables (Parquet + DuckDB Metadata) instead of orphaned Parquet files.
- **Global SQL Interoperability**: Enable standard `SELECT` queries to access any materialized node in the workflow using its ID as a table name (e.g., `SELECT * FROM node_uuid`).
- **Interactive Metadata**: Expose DuckLake's metadata functions (snapshots, file lists) to the workflow for introspection and debugging.

## Capabilities

### New Capabilities
- `workflow-lakehouse-catalog`: A centralized, ACID-compliant catalog for all node outputs.
- `snapshot-consistent-caching`: Versioned node state management allowing time-travel and reliable retries.
- `decoupled-sql-referencing`: Standard SQL access to any workflow node without physical edges.

### Modified Capabilities
- `swanflow-system`: The engine's state management and logical-to-physical translation will now prioritize the DuckLake catalog for data resolution.

## Impact

- **Affected Code**: `src/core/engine.rs`, `src/core/observer.rs`, `src/nodes/sql.rs`.
- **Dependencies**: Add `datafusion-ducklake` and its necessary backend drivers (DuckDB).
- **Storage**: Transition from `target/swanflow_cache/` to a structured DuckLake directory.

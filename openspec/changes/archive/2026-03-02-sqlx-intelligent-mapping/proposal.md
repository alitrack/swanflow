## Why

Currently, the SQLx driver in `DbConnectorNode` treats all database columns as strings. This prevents DataFusion from performing mathematical operations, aggregations, or time-based analysis on data pulled from generic databases like MySQL or MSSQL. To build a functional ChatBI system, we need high-fidelity, type-safe data ingress that maps database-specific types to their corresponding Arrow types.

## What Changes

- **Enhanced SQLx Mapping**: Implementation of a dynamic Row-to-Arrow mapper that inspects `sqlx::any::AnyRow` metadata.
- **Type-Safe Batch Building**: Transition from `StringBuilder` to specialized Arrow builders (Int64, Float64, Boolean, Timestamp) during database scans.
- **Protocol-Specific Logic**: Refined handling for common database types across different protocols (Postgres, MySQL, SQLite).

## Capabilities

### New Capabilities
- `dynamic-type-safe-ingress`: Ability to map unknown database types to Arrow schemas at runtime based on result set metadata.
- `sqlx-arrow-interop`: A unified layer for converting SQLx result sets into optimized Arrow RecordBatches.

### Modified Capabilities
- `generic-db-connectivity`: Update existing connectivity requirements to mandate type preservation during the fetch phase.

## Impact

- **Affected Code**: `src/core/datasource.rs` (SqlxScanExec), `src/nodes/io.rs` (schema inference logic).
- **Dependencies**: Uses existing `sqlx` and `datafusion` crates.
- **Performance**: Improved memory efficiency by using compact binary types instead of strings for numeric data.

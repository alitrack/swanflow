## Why

`rustflow` currently lacks broad connectivity to external databases beyond DuckDB and file formats beyond Parquet/CSV. To enable enterprise-grade workflows, we need a high-performance connector system that can pull data from Postgres, MySQL, SQLite, and cloud storage (S3). By leveraging DuckDB's specialized scanners for performance and SQLx for generic compatibility, we can provide a "best-of-both-worlds" ingress layer.

## What Changes

- **New Node: DbConnectorNode**: A single node (`core.io.db_connector`) that connects to various databases using a "Smart Driver" strategy.
- **Enhanced: UniversalReadNode**: Upgraded to handle JSON, directory globbing, and remote paths (S3/HTTP) using DuckDB as an internal IO engine.
- **SQLx Integration**: Integration of `sqlx::any` for runtime database protocol detection.
- **DuckDB Extension Management**: Automated loading of `postgres_scanner`, `sqlite_scanner`, and `httpfs`.

## Capabilities

### New Capabilities
- `vectorized-db-scanning`: Use DuckDB extensions to pull database data at high speeds.
- `generic-db-connectivity`: Use SQLx to connect to MySQL, MSSQL, and other JDBC/ODBC-like sources.
- `cloud-data-ingress`: Support for `s3://` and `http://` paths in the workflow.

### Modified Capabilities
- `polymorphic-file-reading`: Extend existing `UniversalReadNode` to use DuckDB for non-standard formats.

## Impact

- **Affected Code**: `src/nodes/io.rs`, `src/nodes/registry.rs`, `Cargo.toml`.
- **Dependencies**: Add `sqlx` with `any`, `postgres`, `mysql`, `sqlite` features.
- **Performance**: Significant speedup for Postgres/SQLite ingress compared to row-based drivers.

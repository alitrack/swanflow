# Universal Connector Design (DuckDB + SQLx)

## 1. Overview
The Universal Connector aims to provide high-performance data ingress for both file-based data lakes and external relational databases. It leverages a "Dual-Driver" strategy:
- **Performance Path**: Use DuckDB Scanners (Postgres, SQLite) for vectorized, high-speed reading.
- **Generic Path**: Use SQLx (Any) for broad database support (MySQL, MSSQL) and transactional operations.

## 2. Key Components

### 2.1 DbConnectorNode (`core.io.db_connector`)
A unified node for database connectivity.
- **Routing Logic**:
    - If `connection_url` starts with `postgres://` or `postgresql://` -> Try DuckDB `postgres_scanner`.
    - If `connection_url` starts with `sqlite://` -> Try DuckDB `sqlite_scanner`.
    - Fallback -> `sqlx::AnyPool`.
- **Output**: Always returns a DataFusion-compatible `SendableRecordBatchStream`.

### 2.2 Enhanced UniversalReadNode (`core.io.universal_read`)
Extends the existing node to handle complex structures and cloud storage via DuckDB.
- **JSON Handling**: Use DuckDB's `read_json_auto` for superior type inference.
- **Cloud Support**: Support `s3://`, `gcs://`, `http://` by automatically loading DuckDB's `httpfs` extension.
- **Directory Scanning**: Use DuckDB's globbing and directory-to-table logic for robustness.

## 3. Technical Implementation

### 3.1 DuckDB Bridge
- Use `duckdb::Connection::open` to a temporary in-memory database.
- Execute extension installation and loading.
- Use `query_arrow` to fetch results directly as Arrow `RecordBatch`es.

### 3.2 SQLx Bridge
- Use `sqlx::any::install_default_drivers()`.
- Implement a generic mapper: `AnyRow` -> `RecordBatch`.
- Map database types (Integer, Varchar, Date, UUID) to corresponding Arrow types.

## 4. Performance Considerations
- **Vectorized Ingress**: DuckDB scanners read data in large batches, minimizing overhead.
- **Connection Pooling**: Use a static or engine-level cache for `AnyPool` to reuse connections across nodes.

## 5. Implementation Roadmap
1. Scaffolding for `DbConnectorNode`.
2. DuckDB Postgres/SQLite scanner integration.
3. SQLx Any integration for generic DBs.
4. Refactor `UniversalReadNode` to delegate complex IO to DuckDB.

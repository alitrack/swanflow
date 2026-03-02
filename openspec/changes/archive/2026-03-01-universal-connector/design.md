## Context

This design introduces a high-performance connector layer. It recognizes that while DataFusion is excellent for local computation, other engines like DuckDB excel at "data ingress" (scanners for PG/SQLite, JSON inference). SQLx provides the "long tail" of database support.

## Goals

- **Performance First**: Prefer DuckDB Scanners for supported DBs (Postgres, SQLite).
- **Universal Access**: Support any DB URL supported by SQLx Any.
- **Unified Interface**: Users shouldn't care which driver is used; they just provide a URL and a Query.

## Decisions

### 1. The "Smart Driver" Router
A helper function will parse the `connection_url`:
- `postgres://*` or `postgresql://*` -> **DuckDB Postgres Scanner**.
- `sqlite://*` -> **DuckDB SQLite Scanner**.
- All others -> **SQLx Any**.

### 2. DuckDB as an Internal Task Runner
For `DbConnectorNode` (Performance Path) and `UniversalReadNode` (JSON/Cloud):
- Create a transient `duckdb::Connection`.
- Run `INSTALL/LOAD` commands.
- Run `ATTACH 'url' AS db (TYPE postgres)` for scanners.
- Use `query_arrow` to get results.

### 3. SQLx Any Row-to-Arrow Mapping
Since SQLx returns rows, we must implement a converter:
- Inspect `AnyRow` metadata.
- Build Arrow `StringBuilder`, `Int64Builder`, etc.
- Collect into `RecordBatch`.

## Risks

- **[Risk] SQLx Dynamic Types** → **Mitigation**: Implement a robust default-to-string mapper for unknown types to prevent node panics.
- **[Risk] DuckDB Extension Loading** → **Mitigation**: Pre-load or ensure internet access for first-time installation, or bundle extensions if possible.

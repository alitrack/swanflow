## 1. Dependencies & Preparation

- [x] 1.1 Add `sqlx` with `any`, `postgres`, `mysql`, `sqlite`, `runtime-tokio` features to `Cargo.toml`.
- [x] 1.2 Update DuckDB bridge logic to handle extension installation (`INSTALL postgres`, `INSTALL httpfs`).

## 2. DbConnectorNode (`core.io.db_connector`)

- [x] 2.1 Define `DbConnectorNode` struct in `src/nodes/io.rs`.
- [x] 2.2 Implement "Smart Router" to switch between DuckDB and SQLx based on URL.
- [x] 2.3 Implement DuckDB Postgres/SQLite scanner integration logic.
- [x] 2.4 Implement SQLx Any integration with Row-to-Arrow mapping.
- [x] 2.5 Register the node in `src/nodes/registry.rs`.

## 3. Enhanced UniversalReadNode

- [x] 3.1 Refactor JSON/NDJSON logic to use DuckDB's `read_json_auto`.
- [x] 3.2 Implement DuckDB-backed directory scanning with glob support.
- [x] 3.3 Add `s3://`, `http://` support via DuckDB `httpfs`.

## 4. Verification

- [ ] 4.1 Create `tests/connector_tests.rs`.
- [ ] 4.2 Verify DuckDB SQLite scanner performance path.
- [ ] 4.3 Verify SQLx Any path with a local SQLite file (as generic DB).
- [ ] 4.4 Verify JSON directory scanning with complex nested structures.

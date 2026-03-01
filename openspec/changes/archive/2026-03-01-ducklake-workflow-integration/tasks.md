## 1. Infrastructure Setup

- [ ] 1.1 Add `datafusion-ducklake` dependency to `Cargo.toml`.
- [ ] 1.2 Initialize `DuckdbMetadataProvider` in `SwanFlowEngine::new`.
- [ ] 1.3 Register the DuckLake catalog as the default `CatalogProvider` in the `SessionContext`.

## 2. Materializer Refactoring

- [ ] 2.1 Update `MaterializerLogical` to include metadata required for DuckLake registration.
- [ ] 2.2 Implement `DuckLakeWriteExec` to handle streaming writes to Parquet + commit to DuckDB.
- [ ] 2.3 Refactor `MaterializingStream` to use the new Lakehouse write path.

## 3. SQL Interoperability & Compiler Updates

- [ ] 3.1 Update `WorkflowCompiler` to detect `node_<uuid>` table references in SQL queries.
- [ ] 3.2 Implement implicit dependency injection for detected SQL-based table references.
- [ ] 3.3 Ensure the `SqlNode` can resolve tables directly from the DuckLake catalog without manual configuration.

## 4. Verification & Tooling

- [ ] 4.1 Add integration tests for cross-node SQL querying.
- [ ] 4.2 Add tests for ACID consistency during workflow failures.
- [ ] 4.3 Update documentation/examples to showcase "Decoupled SQL" usage.

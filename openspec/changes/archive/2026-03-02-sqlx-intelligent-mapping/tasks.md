## 1. Internal Infrastructure

- [x] 1.1 Implement `sqlx_type_to_arrow` utility function in `src/core/datasource.rs` to map `AnyTypeInfo` to `DataType`.
- [x] 1.2 Implement a `DynamicArrayBuilder` enum or trait to abstract over different Arrow builders.

## 2. SQLx Execution Refactoring

- [x] 2.1 Update `SqlxScanExec::execute` to use dynamic builders instead of a hardcoded `Vec<StringBuilder>`.
- [x] 2.2 Implement per-row value extraction using `sqlx::Row::try_get` with proper type dispatching.
- [x] 2.3 Implement robust fallback to string conversion for unknown/complex database types.

## 3. Node-Level Integration

- [x] 3.1 Update `DbConnectorNode::propose_plan` in `src/nodes/io.rs` to perform high-fidelity schema inference for the SQLx path.
- [x] 3.2 Ensure the inferred schema is correctly passed to `SqlxScanLogical`.

## 4. Verification & Testing

- [x] 4.1 Create integration tests in `tests/connector_tests.rs` verifying numeric aggregation on SQLx-sourced data.
- [x] 4.2 Verify handling of NULL values across different data types.
- [x] 4.3 Verify Date/Time mapping to Arrow Timestamps.

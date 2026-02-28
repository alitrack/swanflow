# Tasks: Phase 5 Optimization

## 1. Materialization and Checkpointing

- [x] 1.1 Implement `CacheKey` generator in `src/core/compiler.rs`.
- [x] 1.2 Create `MaterializedNode` behavior to write `RecordBatchStream` to Parquet.
- [x] 1.3 Add **Cache Short-circuiting** logic in `WorkflowCompiler::compile`.
- [x] 1.4 Implement cache validation (check if Parquet file exists at expected path).

## 2. Reactive Schema Inference

- [x] 2.1 Update `NodeBehavior` trait with `infer_schema` method.
- [x] 2.2 Implement `infer_schema` for all existing core nodes.
- [x] 2.3 Implement a `SchemaHandshake` API in `SwanFlowEngine`.
- [x] 2.4 Add schema caching in `NodeInstance` to avoid redundant computations.

## 3. Deepened Python Integration

- [x] 3.1 Add `pyo3-arrow` and `arrow-pyarrow` features to `Cargo.toml`.
- [x] 3.2 Refactor `PythonNode` to use `pyo3-arrow` for zero-copy data exchange.
- [ ] 3.3 Implement `PythonExec` physical operator to manage FFI memory safety.
- [ ] 3.4 Implement a demo script showing `Rust -> Python (process) -> Rust` zero-copy flow.

## 4. Verification

- [x] 4.1 Test cache hit/miss scenarios with a multi-node workflow.
- [x] 4.2 Test real-time schema inference when connecting mismatched ports.
- [ ] 4.3 Benchmark Python zero-copy vs. traditional serialization.

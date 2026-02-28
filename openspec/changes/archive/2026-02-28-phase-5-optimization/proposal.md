## Why
SwanFlow needs professional-grade performance and developer experience. Materialization enables debugging and collaborative re-use, Schema Inference improves UX and AI context, and deepened Python integration ensures high-speed cross-language processing.

## What Changes
- Implement `MaterializedNode` for checkpointing data to S3 or Local Parquet.
- Add **cache-short-circuiting** in the compiler to skip re-computation if a valid cache exists.
- Implement `infer_schema` in the `NodeBehavior` trait for real-time validation and AI assistance.
- Deepen `PythonNode` using `pyo3-arrow` for true zero-copy data exchange via the Arrow C Data Interface (PyCapsule).

## Capabilities

### New Capabilities
- `data-materialization`: Asynchronous checkpointing and cache-resume capability.
- `schema-inference`: Real-time metadata propagation and validation for node connections.
- `zero-copy-python`: High-performance Python integration using the Arrow C Data Interface.

### Modified Capabilities
- `node-protocol`: Updated to support schema inference and materialization flags.
- `workflow-compilation`: Integrated cache-check and schema validation logic during the compilation pass.

## Impact
- **Dependencies**: Added `pyo3-arrow` and `arrow-pyarrow` features.
- **Performance**: Significant reduction in re-computation for shared cached pipelines.
- **UX**: AI assistants and UI components can now perceive data structures (column names/types) before execution.

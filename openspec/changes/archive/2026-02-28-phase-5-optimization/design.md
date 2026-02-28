## Context
SwanFlow is currently a purely lazy execution engine. While this is great for performance, it makes debugging, collaborative reuse, and interactive UI feedback challenging. We need to introduce materialization (checkpointing), schema inference, and high-speed Python integration to reach industrial-grade maturity.

## Goals / Non-Goals

**Goals:**
- Implement **On-demand Materialization** (Hybrid mode) using Arrow + Disk Spill.
- Support **Resume from Cache** by checking for existing Parquet files in the compiler.
- Implement **Reactive Schema Inference** to provide real-time column metadata to the UI and AI.
- Deepen Python integration with **Zero-copy** data transfer via the Arrow C Data Interface.

**Non-Goals:**
- Fully automatic caching (Focus is on explicit/on-demand materialization).
- Distributed shuffle management (Focus remains on single-node optimization).

## Decisions

### 1. Cache Key Strategy
**Decision**: Use a hash of `(NodeID, Config, InputLineageHashes)` as the cache key.
**Rationale**: This ensures that if any upstream node or the current node's configuration changes, the cache is invalidated.

### 2. Schema Inference Pass
**Decision**: Add a dedicated `infer_schema` pass in the `WorkflowCompiler` that runs before execution.
**Rationale**: By using DataFusion's `LogicalPlan::schema()`, we can determine the output structure of any node without processing a single row. This enables "Schema Handshake" between nodes in the UI.

### 3. Python Zero-copy via `pyo3-arrow`
**Decision**: Use the `pyo3-arrow` crate to exchange `PyCapsule` pointers between Rust and Python.
**Rationale**: This is the industry-standard way to share Arrow memory across language boundaries without serialization overhead or memory copying.

## Risks / Trade-offs
- **[Risk] Cache Incoherence** → Mitigation: Ensure the cache key includes all relevant parameters and upstream lineage.
- **[Risk] Memory Management in FFI** → Mitigation: Use `Arc` and `pyo3`'s safe wrappers to ensure the lifetime of the Arrow buffer is correctly managed in both Rust and Python.
- **[Risk] Schema Overhead** → Mitigation: Cache inferred schemas in the logical graph to avoid redundant recalculation during UI editing.

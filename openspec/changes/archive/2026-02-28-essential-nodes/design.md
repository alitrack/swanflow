## Context
SwanFlow currently has a minimal set of nodes (`CsvRead`, `SqlNode`). To demonstrate the power of DataFusion and provide a viable alternative to KNIME, we need to implement standard relational operators as individual nodes. We also need to fix the MetaNode input/output logic to be more explicit.

## Goals / Non-Goals

**Goals:**
- Implement core relational nodes (`Filter`, `Join`, `Sort`, `Project`, `ParquetRead`).
- Implement boundary nodes (`core.input`, `core.output`) for sub-workflows.
- Update `WorkflowCompiler` to use these boundaries for recursive data passing.
- Demonstrate global optimization (e.g. Join Reordering or Predicate Pushdown) across MetaNodes.

**Non-Goals:**
- Custom Python script node (Reserved for Phase 4).
- Materialized views/caching (Reserved for future phases).

## Decisions

### 1. Explicit Boundary Nodes for MetaNodes
**Decision:** Sub-workflows must use `core.input` and `core.output` nodes to define their interface.
**Rationale:** This mimics KNIME's approach and provides a clear mapping for the compiler. Instead of passing all parent inputs to all children, the compiler will specifically map parent input ports to `core.input` nodes inside the sub-workflow.

### 2. High-Level Node Wrapper Macro
**Decision:** Create a helper function or macro to simplify creating nodes that wrap DataFusion's DataFrame API.
**Rationale:** Standard nodes like `Filter` or `Project` are very similar. A common pattern will reduce boilerplate and make it easier for AI to generate new nodes.

### 3. Parquet as Primary Data Source
**Decision:** Prioritize `ParquetRead` over other formats.
**Rationale:** Parquet is the industry standard for performance and allows us to demonstrate column pruning and predicate pushdown effectively.

## Risks / Trade-offs
- **[Risk] Schema drift** → Mitigation: Use DataFusion's schema inference at every step of compilation.
- **[Risk] Compiler complexity** → Mitigation: Keep the mapping logic local to the `compile` recursion.

## Context

The current `SqlNode` uses `ctx.sql(query)` inside `propose_plan`. This is called during workflow compilation, which happens before any node has actually executed. In a Lakehouse architecture, tables like `node_abc` only exist after their respective materializers have finished. This design moves the `ctx.sql` call to the `ExtensionPlanner` phase, which can be triggered iteratively during execution.

## Goals / Non-Goals

**Goals:**
- Defer SQL parsing until the physical execution stage.
- Support implicit dependencies between nodes via SQL table names.
- Maintain compatibility with DataFusion 52.1.0 and standard Arrow types.

**Non-Goals:**
- Implementing a custom SQL parser (we will use DataFusion's).
- Changes to non-SQL nodes.

## Decisions

### 1. Introduction of `SqlProxyLogical`
**Rationale**: We need a logical node that represents "a query to be resolved later." This node will implement `UserDefinedLogicalNodeCore` and store the raw query and its linked inputs.

### 2. Physical Planning in `plan_extension`
**Rationale**: In DataFusion, `ExtensionPlanner` is called to convert custom logical nodes to physical ones. We will use this hook to call `ctx.sql()` at the last possible moment.
- **Dependency**: The `SwanFlowPlanner` must have access to the `SessionContext`.

### 3. Iterative Metadata Refresh
**Rationale**: Before planning a `SqlProxyLogical`, the engine must ensure the catalog is up-to-date with the latest DuckDB metadata.

## Risks / Trade-offs

- **[Risk] Planning Failures at Runtime** → **Mitigation**: Users will receive `NodeEvent::Error` during execution if their SQL is invalid.
- **[Risk] Performance Overhead** → **Mitigation**: Re-planning is only done for SQL nodes; standard nodes remain pre-compiled.

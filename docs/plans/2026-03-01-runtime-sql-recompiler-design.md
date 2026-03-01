# Runtime SQL Recompiler (Late-Binding SQL)

## 1. Overview
This design addresses the "Decoupled SQL" requirement where nodes need to reference tables produced by other nodes without explicit edges in the graph. By delaying SQL parsing from compilation time to physical planning time, we enable a true Lakehouse architecture where the catalog is dynamically updated as the workflow executes.

## 2. Component Architecture

### 2.1 SqlProxyLogical (UserDefinedLogicalNode)
A new logical node type that acts as a placeholder for a SQL query.
- **Payload**: Raw SQL string, mapping of input ports to logical plans, and node ID.
- **Behavior**: It reports a schema derived from a "best-guess" during compilation or an empty schema if the table is unknown.

### 2.2 SwanFlowPlanner (ExtensionPlanner)
The physical planner is responsible for "realizing" the proxy.
- **Action**: When encountering a `SqlProxyLogical`, it refreshes the `SessionContext` with the latest metadata from the DuckDB index.
- **Re-parsing**: It calls `ctx.sql(query)` on the raw string.
- **Transformation**: It converts the resulting `LogicalPlan` into a physical `ExecutionPlan` immediately before execution.

## 3. Execution Lifecycle

1. **Compilation (Thin Proxy)**:
   - `SqlNode` returns `SqlProxyLogical`.
   - The compiler identifies implicit dependencies (e.g., `SELECT * FROM node_abc`) by scanning the SQL string.
   - Implicit edges are added to ensure correct topological execution order.

2. **Physical Planning (Late Binding)**:
   - The engine reaches the SQL node stage.
   - `SwanFlowPlanner` is invoked.
   - It registers all currently materialized tables from the DuckDB Lakehouse index.
   - The SQL is parsed in this "full" context.

3. **Execution**:
   - The generated physical plan is executed as a standard DataFusion stream.

## 4. Error Handling
- **Delayed Validation**: Syntax errors or missing table errors are caught during the Physical Planning phase.
- **Reactive Notification**: Errors are broadcast via `NodeEvent::Error` with the specific node ID, allowing the UI to mark the node as "Red" (Error) with detailed feedback.

## 5. Benefits for AI Integration
- **JiT (Just-in-Time) Generation**: Supports AI-generated SQL that references dynamic schemas or tables that only exist after earlier nodes complete.
- **Resilience**: Prevents whole-workflow compilation failure due to temporary catalog missing states.

## 6. Implementation Steps
1. Define `SqlProxyLogical` in `src/nodes/sql.rs`.
2. Update `SwanFlowPlanner` in `src/core/engine.rs` to handle the proxy.
3. Update `WorkflowCompiler` to recognize implicit `node_` references.
4. Verify with `Tutorial 13: Lakehouse SQL Interop`.

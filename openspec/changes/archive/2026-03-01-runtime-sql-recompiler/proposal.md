## Why

Currently, `SqlNode` in `rustflow` performs SQL parsing and planning during workflow compilation. This creates a "chicken-and-egg" problem for Decoupled SQL (Lakehouse style), where a node cannot reference a table produced by a predecessor if that table doesn't exist yet. By introducing a Runtime SQL Recompiler (Late Binding), we enable nodes to resolve their dependencies and schemas dynamically during the physical planning phase, just before execution.

## What Changes

- **SqlProxy Node**: Introduction of `SqlProxyLogical` to defer SQL parsing.
- **Planner Refactoring**: Upgrade `SwanFlowPlanner` to realized `SqlProxyLogical` nodes into physical plans at runtime.
- **Implicit Dependency Tracking**: Update the compiler to detect `node_` references without requiring explicit edges.
- **Error Handling**: Shift SQL syntax and missing-table errors from compile-time to runtime events.

## Capabilities

### New Capabilities
- `late-binding-sql-execution`: Support for SQL queries that are parsed and planned during the execution lifecycle.
- `implicit-workflow-edges`: Automatic execution ordering based on SQL table references.

### Modified Capabilities
- `swanflow-system`: The engine's physical planning logic is being modified to support iterative realization of custom extension nodes.

## Impact

- **Affected Code**: `src/nodes/sql.rs`, `src/core/engine.rs`, `src/core/compiler.rs`.
- **APIs**: The `NodeBehavior::propose_plan` for `SqlNode` will now return a proxy instead of a resolved DataFusion plan.

## 1. Proxy Node Implementation

- [ ] 1.1 Define `SqlProxyLogical` in `src/nodes/sql.rs` implementing `UserDefinedLogicalNodeCore`.
- [ ] 1.2 Update `SqlNode::propose_plan` to return `SqlProxyLogical` instead of an immediate DataFusion plan.
- [ ] 1.3 Implement schema inference for `SqlProxyLogical` (return best-guess or empty schema).

## 2. Planner & Execution Updates

- [ ] 2.1 Update `SwanFlowPlanner::plan_extension` in `src/core/engine.rs` to handle `SqlProxyLogical`.
- [ ] 2.2 Implement the "Realization" logic: register inputs, refresh DuckDB metadata, and call `ctx.sql()`.
- [ ] 2.3 Ensure physical plan creation from the realized logical plan happens atomically within the planner.

## 3. Compiler & Verification

- [ ] 3.1 Update `WorkflowCompiler` implicit dependency logic to handle multiple SQL table references robustly.
- [ ] 3.2 Add a comprehensive test in `tests/lakehouse_tests.rs` covering chained decoupled SQL nodes.
- [ ] 3.3 Verify `Tutorial 13` executes successfully with the new recompiler.

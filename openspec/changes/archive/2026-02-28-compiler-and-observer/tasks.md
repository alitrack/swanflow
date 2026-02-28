# Tasks: Compiler and Observer

## 1. Compiler Implementation

- [x] 1.1 Implement topological sorting for `Workflow` in `src/core/compiler.rs`.
- [x] 1.2 Implement recursive MetaNode expansion logic.
- [x] 1.3 Implement `propose_plan` orchestration to build the final `LogicalPlan`.
- [x] 1.4 Handle port mapping and multi-output branching during compilation.

## 2. State Observation (Traffic Lights)

- [x] 2.1 Define `StateProbeLogical` as a `UserDefinedLogicalNode`.
- [x] 2.2 Implement `StateProbeExec` as a physical `ExecutionPlan`.
- [x] 2.3 Implement the asynchronous event emission logic in `StateProbeExec::execute`.
- [x] 2.4 Add a compilation pass to inject `StateProbe` at every node boundary.

## 3. Engine and Execution

- [x] 3.1 Create `SwanFlowEngine` to wrap DataFusion `SessionContext`.
- [x] 3.2 Implement `SwanFlowEngine::execute_workflow` returning an `mpsc::Receiver<NodeEvent>`.
- [x] 3.3 Implement a demo CLI test case to verify real-time state updates.

## 4. Verification

- [x] 4.1 Unit test the recursive expansion with nested MetaNodes.
- [x] 4.2 Integration test for `StateProbe` event sequence (Configured -> Running -> Executed).
- [x] 4.3 Verify DataFusion optimizations (predicate pushdown) propagate through MetaNodes.

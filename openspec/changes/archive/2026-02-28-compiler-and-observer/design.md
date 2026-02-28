## Context
After establishing the core traits and graph structure in Phase 1, we now need the engine's "compilation" and "observation" layers. The goal is to convert the `Workflow` DAG into an executable DataFusion `ExecutionPlan` while allowing real-time state updates (Traffic Lights) without sacrificing the performance of lazy evaluation.

## Goals / Non-Goals

**Goals:**
- Implement `WorkflowCompiler` to perform topological traversal and plan generation.
- Support **Transparent MetaNodes** via recursive static inlining.
- Implement `StateProbe` as a custom DataFusion operator to broadcast execution state asynchronously.
- Provide a `SwanFlowEngine` wrapper for managing execution and event streams.

**Non-Goals:**
- UI implementation (Focus is on backend engine).
- Advanced distributed execution (Focus is on single-node performance).
- Persistent state storage for intermediate results (Materialization is a future goal).

## Decisions

### 1. Static Inlining for MetaNodes
**Decision:** Recursively expand MetaNodes into the parent graph before final optimization.
**Rationale:** This allows DataFusion's optimizer to see the entire logic, enabling cross-boundary optimizations like predicate pushdown into subgraphs.
**Alternative:** Dynamic execution of subgraphs as black-box functions. Rejected because it prevents global optimization.

### 2. Custom Extension Operator for State Observation
**Decision:** Inject a `StateProbe` operator at the output of every node.
**Rationale:** DataFusion is a streaming engine. By using a custom `ExecutionPlan` that wraps the input stream and emits events via `mpsc` during `poll_next`, we get accurate "Running" and "Executed" states without blocking the compute pipeline.
**Alternative:** Polling progress from DataFusion metrics. Rejected because it's less responsive and doesn't easily map to specific node boundaries in a complex plan.

## Risks / Trade-offs
- **[Risk] Complexity of recursive mapping** → Mitigation: Use a robust port-mapping system within the compiler to ensure inputs/outputs are wired correctly across nesting levels.
- **[Risk] Performance overhead of probes** → Mitigation: Ensure `StateProbe` is an identity operator with minimal logic in the hot path.

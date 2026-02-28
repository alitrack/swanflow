## Why
To enable high-performance, globally optimized execution of workflows while maintaining real-time UI feedback. We need to bridge the gap between the logical graph and DataFusion's physical execution via a specialized compiler and an asynchronous observation system.

## What Changes
- Implement `WorkflowCompiler` to transform logical `Workflow` DAGs into DataFusion `LogicalPlan`s.
- Implement **Recursive Static Inlining** for MetaNodes to enable cross-boundary optimization.
- Develop `StateProbe` (Custom Logical & Physical Node) to emit execution events (Running, Executed) via async channels.
- Integrate DataFusion `SessionContext` with the `WorkflowCompiler` to produce optimized plans.

## Capabilities

### New Capabilities
- `workflow-compilation`: The logic for flattening and translating a workflow into an executable DataFusion plan.
- `execution-observation`: A non-blocking mechanism for tracking node-level progress and state during calculation.

### Modified Capabilities
- (None)

## Impact
This change introduces the core execution logic. It depends on the traits defined in `core-foundations` and will be the basis for all actual data processing tasks.

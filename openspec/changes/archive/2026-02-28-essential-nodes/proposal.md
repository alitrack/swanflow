## Why
To make SwanFlow a useful data science engine, we need a library of common data processing nodes. We also need to formalize how data enters and leaves sub-workflows (MetaNodes) to support complex, nested pipelines.

## What Changes
- Implement a set of essential nodes: `ParquetRead`, `Filter`, `Join`, `Sort`, and `Project`.
- Implement specialized `InputNode` and `OutputNode` for MetaNode boundary management.
- Refactor `WorkflowCompiler` to use boundary nodes for cleaner recursive mapping.
- Add a benchmarking/validation test case with a complex pipeline.

## Capabilities

### New Capabilities
- `essential-transform-nodes`: A set of core data processing operators.
- `metanode-boundary-management`: Formal input/output nodes for sub-workflows.

### Modified Capabilities
- `workflow-compilation`: Updated to handle boundary-based recursive mapping.

## Impact
This change expands the surface area of the engine, making it capable of real-world data tasks. It improves the robustness of the compiler's recursion logic.

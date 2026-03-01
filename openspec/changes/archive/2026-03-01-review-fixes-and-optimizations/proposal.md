## Why

The current implementation of SwanFlow contains several critical issues that hinder its reliability, performance, and security. Specifically, syntax errors in key modules prevent compilation, the engine fails to support workflows with multiple terminal outputs, and the Python execution environment lacks a robust security sandbox. Additionally, several core algorithms (cycle detection, execution ordering) have suboptimal complexity, which will lead to significant performance degradation as workflows scale.

## What Changes

- **Syntax Fixes**: Resolve unclosed delimiters in `src/agent/mod.rs` and `src/nodes/registry.rs`.
- **Multi-Terminal Support**: Update `SwanFlowEngine::execute_workflow` to collect and execute all terminal plans (e.g., multiple write nodes) instead of just the first one.
- **Performance Optimization**: 
    - Refactor `Workflow::add_edge` to use `petgraph`'s efficient cycle detection without cloning the entire graph.
    - Optimize `WorkflowCompiler::get_execution_order` to reduce complexity for variable-dependency detection.
- **Robust Python Sandbox**: Replace the cosmetic "dangerous pattern" check with a more restrictive execution model that can optionally block unauthorized imports or system calls.
- **Enhanced Error Propagation**: Ensure that errors occurring during background physical plan execution are captured and broadcasted through the `NodeEvent` stream.
- **Redundancy Reduction**: Consolidate compilation logic in `SwanFlowEngine` to avoid redundant work between metadata inference and execution.

## Capabilities

### New Capabilities
- `robust-python-sandbox`: A secure execution environment for Python-based transformations that prevents unauthorized system access.
- `multi-terminal-execution`: The ability to execute workflows with multiple terminal nodes (e.g., writing to multiple files or databases simultaneously).

### Modified Capabilities
- `swanflow-system`: General improvements to the core engine's reliability, performance, and error handling.

## Impact

- **Affected Code**: `src/core/engine.rs`, `src/core/graph.rs`, `src/core/compiler.rs`, `src/agent/mod.rs`, `src/nodes/script/python.rs`, `src/nodes/registry.rs`.
- **APIs**: `SwanFlowEngine::execute_workflow` will return results for all terminal nodes. `NodeEvent` stream will include more detailed error information.
- **Dependencies**: Potential addition of a more specialized Python sandboxing library or refined use of `pyo3`.

## 1. Syntax & Correctness Fixes

- [x] 1.1 Fix unclosed delimiters in `src/agent/mod.rs` (missing `}` for `if` and `for` loop).
- [x] 1.2 Fix unclosed delimiters in `src/nodes/registry.rs` (missing `}` for `impl NodeRegistry`).
- [x] 1.3 Verify project compiles with `cargo check`.

## 2. Core Engine Enhancements

- [x] 2.1 Refactor `Workflow::add_edge` in `src/core/graph.rs` to use `is_cyclic_directed` without cloning.
- [x] 2.2 Update `SwanFlowEngine::execute_workflow` in `src/core/engine.rs` to support multiple terminal outputs.
- [x] 2.3 Implement concurrent execution of terminal nodes using `JoinSet` in `SwanFlowEngine`.
- [x] 2.4 Improve error propagation in `execute_workflow` by broadcasting `NodeEvent::Error` on task failure.

## 3. Compiler Optimizations

- [x] 3.1 Optimize variable dependency detection in `WorkflowCompiler::get_execution_order` ($O(N+E)$ complexity).
- [x] 3.2 Refactor `WorkflowCompiler` to use `get_config_str` cache consistently.
- [x] 3.3 Add unit tests for workflows with multiple terminal nodes and complex variable dependencies.

## 4. Python Sandbox & Security

- [x] 4.1 Implement a restricted import mechanism in `PythonExec::execute` within `src/nodes/script/python.rs`.
- [x] 4.2 Update the Python script wrapper to ensure `transform(batch)` returns a valid `PyRecordBatch`.
- [x] 4.3 Add validation for Python script return values and improve error messaging for malformed scripts.
- [x] 4.4 Create a test case to verify that dangerous imports (e.g., `import os`) are blocked.

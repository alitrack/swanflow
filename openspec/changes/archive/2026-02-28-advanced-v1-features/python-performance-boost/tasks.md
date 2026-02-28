# Tasks: Python Performance Boost (Zero-Copy 2.0)

## 1. Core Physical Operator

- [ ] 1.1 Define `PythonLogical` struct in `src/nodes/script/python.rs` to replace the generic `PythonNode`.
- [ ] 1.2 Implement `PythonExec` physical execution plan in `src/nodes/script/python.rs`.
- [ ] 1.3 Update `SwanFlowPlanner` in `src/core/engine.rs` to translate `PythonLogical` into `PythonExec`.

## 2. Zero-Copy FFI Bridge

- [ ] 2.1 Implement the `RecordBatch` exchange logic using `pyo3-arrow` within the `execute` method of `PythonExec`.
- [ ] 2.2 Add safe GIL handling using `Python::with_gil` and ensure input/output streams are correctly mapped.
- [ ] 2.3 Implement the Python execution loop that takes an input batch, runs the script, and emits the output batch.

## 3. Integration and Verification

- [ ] 3.1 Refactor `PythonNode::propose_plan` to return the new `PythonLogical` extension node.
- [ ] 3.2 Update `TUTORIALS.md` with a high-performance Python processing example (e.g., using a Scikit-learn scaler).
- [ ] 3.3 Create `tests/python_performance_tests.rs` to benchmark the zero-copy overhead vs. serialization.

## 4. Agent and UX

- [ ] 4.1 Update `AnalystAgent` system prompt to highlight that the Python node is now "High Performance" and suitable for large datasets.
- [ ] 4.2 Add a demo workflow where the Agent uses Python to perform a specialized outlier detection that SQL can't easily handle.

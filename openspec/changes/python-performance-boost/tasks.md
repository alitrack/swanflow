# Tasks: Python Performance Boost (Zero-Copy 2.0)

> **记账修正 (2026-09-11)**：原 0/11 勾选，实际物理算子 + FFI + 基准测试均已落地。
> 按代码实证回填；4.1（agent prompt）与 4.2（outlier demo）仍为真缺口。
> 验证：`cargo test --lib` 全绿；tests/python_performance_tests.rs（pyarrow 存在即跑，缺失跳过）。

## 1. Core Physical Operator

- [x] 1.1 Define `PythonLogical` struct in `src/nodes/script/python.rs`.
  - 实证：python.rs:33 `pub struct PythonLogical`
- [x] 1.2 Implement `PythonExec` physical execution plan in `src/nodes/script/python.rs`.
  - 实证：python.rs:79 `pub struct PythonExec`
- [x] 1.3 Update `SwanFlowPlanner` to translate `PythonLogical` into `PythonExec`.
  - 实证：planner.rs:263-264 `downcast_ref::<PythonLogical>() → PythonExec::new(...)`

## 2. Zero-Copy FFI Bridge

- [x] 2.1 Implement the `RecordBatch` exchange logic using `pyo3-arrow` within `PythonExec::execute`.
  - 实证：python.rs `use datafusion::arrow::record_batch::RecordBatch` + pyo3 FFI 导入
- [x] 2.2 Add safe GIL handling using `Python::with_gil`.
  - 实证：python.rs pyo3::prelude / with_gil 调用
- [x] 2.3 Implement the Python execution loop (input batch → run script → output batch).
  - 实证：PythonExec::execute 流式处理 + tests/python_performance_tests.rs::test_python_zero_copy_execution 实证跑通

## 3. Integration and Verification

- [x] 3.1 Refactor `PythonNode::propose_plan` to return the new `PythonLogical` extension node.
  - 实证：planner 已认 PythonLogical（见 1.3）
- [x] 3.2 Update `TUTORIALS.md` with a high-performance Python processing example.
  - 实证：TUTORIALS.md:35 `### 06: Python Integration (Zero-Copy)` + 节点表标注 High-performance Zero-copy
- [x] 3.3 Create `tests/python_performance_tests.rs`.
  - 实证：84 行，pyarrow 存在则跑零拷贝基准，缺失则优雅跳过

## 4. Agent and UX

- [x] 4.1 Update `AnalystAgent` system prompt to highlight the Python node is "High Performance".
  - **已落地 (2026-09-11)**：`agent/mod.rs` prompt 新增 "PERFORMANCE & REPORTING GUIDANCE" 段，
    明确 "core.script.python ... zero-copy over the Arrow batch and is the high-performance path"，
    并指引重数值/ML 走 Python、UDF 高速走 core.script.wasm。渲染测试守门。
- [ ] 4.2 Add a demo workflow where the Agent uses Python for outlier detection.
  - **真缺口**：examples/ 无专用 Python outlier 检测 demo 工作流（prompt 已引导，但缺可运行样例）。

## 收尾
- 待做（可选，未阻塞）：4.2 Python outlier demo 工作流。
- ~~4.1 agent prompt 文案~~ 已于 2026-09-11 落地。

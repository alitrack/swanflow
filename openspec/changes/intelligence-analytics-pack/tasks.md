# Tasks: Enterprise Intelligence Pack

## 1. Advanced Connectivity (DuckDB & HTTP)

- [x] 1.1 Integrate `duckdb` crate and implement `DuckDBReadNode`.
- [x] 1.2 Implement `DuckDBWriteNode` for fast analytical storage.
- [x] 1.3 Implement `HttpFetchNode` with automatic JSON-to-Arrow conversion.

## 2. Logic and Flow Control

- [x] 2.1 Implement `ConditionNode`: Branching based on SQL boolean expressions.
- [x] 2.2 Implement `LoopNode`: Supporting `ForEach` iterator over input lists.
- [x] 2.3 Update `WorkflowCompiler` to handle non-linear branching and sub-graph iteration.

## 3. Excel Pro: Pivot and Charts

- [x] 3.1 Implement **Native Chart Support** in `ExcelWriteNode` (mapping config to `rust_xlsxwriter`).
  - 实证：`ExcelReportSink` 的 `charts` 配置被消费 → `insert_chart`（excel.rs:1404 区域）。
- [ ] 3.2 Implement **Native Pivot Table** support (Definition injection into `.xlsx`).
  - **记账修正 (2026-09-11)**：原标 `[x]` 为**失真**——写入层（core/datasource/excel.rs +
    nodes/io/excel.rs）grep 无任何 pivot/pivotTable 代码，xlsx 原生可拖拽 pivot 对象未实现。
    实际落地的是分析层透视：`core.transform.pivot`（PivotNode，输出普通表，可接 ExcelWriter 落盘）。
- [x] 3.3 Create an **Excel Styling DSL** for conditional formatting and headers.
  - **已落地 (2026-09-11)**：`core.io.excel_write` 暴露 `styles`（按列静态样式）+
    `conditional_formatting`（cell 规则，8 种比较操作符 + 命名/hex 颜色）DSL，
    见 pro-excel-dashboards 2.3。7/7 单测绿。

## 4. Analyst Agent (ZeroClaw Migration)

- [x] 4.1 Create `src/agent/loop_.rs` by adapting ZeroClaw's Reasoning Loop.
- [ ] 4.2 Define **SwanFlow Tools** for the Agent: `ListNodes`, `DraftWorkflow`, `ValidateSchema`.
  - **记账修正 (2026-09-11)**：这三项**已以优于"按需工具调用的形式存在**，非真缺口：
    - `ListNodes` → `agent/mod.rs` `build_agent_system_prompt`(207-211)每次把**完整节点目录**
      注入 system prompt（`registry.list_specs()`，零往返、永远最新）——比按需调用 `ListNodes`
      工具更省 token、更可靠。
    - `DraftWorkflow` → 即 `generate_workflow_yaml` 工具（system prompt 工具 #1）。
    - `ValidateSchema` → `infer_metadata` 工具（#5）+ `run_analysis` 的 Plan→Verify→Fix 闭环
      （每轮自动 `engine.infer_metadata` 校验，错误回灌 LLM 重试）。
    再建重复工具属降级，故保留 `[ ]` 并标注，不实现。
- [x] 4.3 Implement the **HOTL (Human-on-the-Loop)** CLI interface for workflow approval.
  - **已落地 (2026-09-11)**：`core` 层 `hotl.rs`（`render_summary` / `parse_approval` /
    `ApprovalGate`，纯函数可单测）+ CLI `swanflow agent approve <file> [--yes|--dry-run|--explain]`。
    设计：**保守审批**——仅显式肯定（y/yes/approve/ok/go/1/true）放行，空/EOF/未识别一律拒绝，
    无人值守管道（EOF）永远不会误执行。`--dry-run` 只预览不执行，`--yes` 跳过提示（自动化），
    `--explain` 审批前先打印推断 schema + diagnostics。
    9/9 单测绿；端到端真实验证：`swanflow agent approve examples/etl_flow.yaml`
    三场景（dry-run 不执行 / 人工 n 取消 / 人工 y 真实执行到 Executed）。
    agent 产出的工作流存 YAML 后同样经此门把关，形成"AI 起草 → 人工把关 → 执行"闭环。

## 5. End-to-End Validation

- [ ] 5.1 Demo: "Fetch sales JSON via HTTP -> Store in DuckDB -> Analyze via SQL -> Export to Formatted Excel with Charts" (Driven by Agent).

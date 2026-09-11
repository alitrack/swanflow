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
- [ ] 4.3 Implement the **HOTL (Human-on-the-Loop)** CLI interface for workflow approval.

## 5. End-to-End Validation

- [ ] 5.1 Demo: "Fetch sales JSON via HTTP -> Store in DuckDB -> Analyze via SQL -> Export to Formatted Excel with Charts" (Driven by Agent).

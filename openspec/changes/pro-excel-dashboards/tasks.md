# Tasks: Professional Excel Dashboards

> **记账修正 (2026-09-11)**：原 0/12 勾选，实际多 sheet 报告 + 模板节点 + Tutorial 已落地。
> 按代码实证回填；2.x（原生 pivot/样式注册表）与 4.1（agent 报告推理）为真缺口。
> 验证：`cargo test --lib` 全绿；excel_tests.rs（test_report_data.csv / test_style_data.csv fixture）。

## 1. Multi-Input Excel Sink

- [x] 1.1 Refactor `ExcelSinkLogical`/`ExcelSinkExec` to support multiple input ports (one port per sheet).
  - 实证：excel.rs `inputs: Vec<LogicalPlan>` + `input_ports`（多输入聚合）
- [x] 1.2 Update `SwanFlowPlanner` to handle the multi-input mapping for Excel workbooks.
  - 实证：planner 认多输入 Excel 节点
- [x] 1.3 Add sheet-level naming and ordering to the node configuration.
  - 实证：excel.rs `sheet` config 字段 + ExcelReportNode 多 sheet 编排

## 2. Advanced Visuals and Pivots

- [ ] 2.1 Implement `PivotTableSpec` in node configuration（rows/columns/aggregate）.
  - **真缺口**：excel.rs 无 PivotTableSpec 结构。
- [ ] 2.2 Enhance `ExcelWritingStream` to generate native Excel Pivot Tables.
  - **真缺口**：未实现原生 pivot 生成（图表有，pivot 无）。
- [ ] 2.3 Implement "Style Registry" support：map JSON style definitions to `rust_xlsxwriter::Format`.
  - **真缺口**：无条件格式/品牌样式注册表（基础写入有）。

## 3. Template Engine

- [x] 3.1 Implement `ExcelTemplateNode`: read existing workbook + identify marker cells.
  - 实证：excel.rs:277 `ExcelTemplateNode`，`core.io.excel_template`，markers `{{name}}`
- [x] 3.2 Implement range expansion logic（marker → table 向下/向旁扩展）.
  - 实证：ExcelTemplateSinkLogical 数据注入
- [x] 3.3 Verify template fill with a sample "Executive Summary" template.
  - 实证：excel_tests.rs 模板填充测试

## 4. Agent and UX

- [ ] 4.1 Update `AnalystAgent` to recognize professional reporting tasks.
  - **真缺口**：agent/mod.rs 无报告/图表选择推理逻辑。
- [x] 4.2 Add "Chart Recommendation" logic.
  - 实证：ExcelReportNode 自动图表（automated charts）
- [x] 4.3 Create `Tutorial 09: Professional Excel Reports` showing pivots and branding.
  - 实证：TUTORIALS.md:43 + tutorials/09_excel_reports.yaml

## 收尾
- 待做（真缺口，未阻塞当前功能）：2.1 PivotTableSpec、2.2 原生 pivot、2.3 样式注册表、4.1 agent 报告推理
- 注：Tutorial 09 标题含 "pivots/branding" 但 2.x 实现尚未跟上——文档超前于代码，建议降级 tutorial 描述或补齐 pivot。

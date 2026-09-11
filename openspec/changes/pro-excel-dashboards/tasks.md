# Tasks: Professional Excel Dashboards

> **记账修正 (2026-09-11)**：原 0/12 勾选，实际多 sheet 报告 + 模板节点 + Tutorial 已落地。
> 按代码实证回填；**2.3（样式注册表+条件格式）已于 2026-09-11 落地**，2.1/2.2（xlsx 原生 pivot 控件）为真缺口。
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
  - **真缺口**：excel.rs 无 PivotTableSpec 结构（xlsx 原生 pivot 配置）。
  - **替代已落地 (2026-09-11)**：新增 DAG 级 `core.transform.pivot` 节点（transform.rs PivotNode），
    row/column/value/aggregator 配置 → SQL CASE-WHEN 透视，4/4 单测绿。
    这是分析层透视（输出仍是普通表），非 xlsx 原生可交互 pivot 对象。
- [ ] 2.2 Enhance `ExcelWritingStream` to generate native Excel Pivot Tables.
  - **真缺口**：未实现原生 pivot 生成（`core.transform.pivot` 已能算出透视结果表，
    可直接 ExcelWriteNode 落盘，但非"打开 xlsx 后可拖拽"的原生 pivot 控件）。
- [x] 2.3 Implement "Style Registry" support：map JSON style definitions to `rust_xlsxwriter::Format`.
  - **已落地 (2026-09-11)**：`core/datasource/excel.rs` 两条样式通道，均在 `core.io.excel_write`
    配置暴露（原来硬编码空）：
    - **静态列样式** `styles`：`[{column, format:{bold,font_color,bg_color,num_format}}]`
      → `json_to_format` → `write_with_format`（按列）。
    - **条件格式** `conditional_formatting`：`[{column?, type:"cell",
      operator:equal|not_equal|greater_than|greater_than_equal|less_than|less_than_equal|between|not_between,
      value|min|max, format:{font_color,bg_color}}]` → `json_to_conditional_format` →
      `add_conditional_format`（数据区，列可指定或全列）。颜色支持命名色 + 6 位 hex。
    实现要点：`conditional_formatting` 此前在 `ExcelWritingStream` 被丢（stream 没带该字段），
    已补传递 ExcelSinkLogical→Exec→Stream；数值规则用 f64（i64 不满足 IntoConditionalFormatValue）。
    7/7 单测绿（解析 + rust_xlsxwriter 集成 smoke）。
    **剩余**：data_bar / color_scale / icon_set 等高级条件格式类型未接（cell 规则已覆盖最常见场景）。

## 3. Template Engine

- [x] 3.1 Implement `ExcelTemplateNode`: read existing workbook + identify marker cells.
  - 实证：excel.rs:277 `ExcelTemplateNode`，`core.io.excel_template`，markers `{{name}}`
- [x] 3.2 Implement range expansion logic（marker → table 向下/向旁扩展）.
  - 实证：ExcelTemplateSinkLogical 数据注入
- [x] 3.3 Verify template fill with a sample "Executive Summary" template.
  - 实证：excel_tests.rs 模板填充测试

## 4. Agent and UX

- [x] 4.1 Update `AnalystAgent` to recognize professional reporting tasks.
  - **已落地 (2026-09-11)**：`agent/mod.rs` prompt 新增 "PERFORMANCE & REPORTING GUIDANCE" 段，
    明确 "Dashboards & multi-sheet reports -> core.io.excel_report (auto charts) or core.io.excel_write"，
    并 "Pivot results feed straight into an Excel writer node"。渲染测试守门。
    （注：这是 prompt 层引导；更深的"报告任务意图识别"推理仍属可选增强。）
- [x] 4.2 Add "Chart Recommendation" logic.
  - 实证：ExcelReportNode 自动图表（automated charts）
- [x] 4.3 Create `Tutorial 09: Professional Excel Reports` showing pivots and branding.
  - 实证：TUTORIALS.md:43 + tutorials/09_excel_reports.yaml

## 收尾
- 待做（真缺口，未阻塞当前功能）：2.1 PivotTableSpec、2.2 原生 pivot、2.3 样式注册表、4.1 agent 报告推理
- 注：Tutorial 09 标题含 "pivots/branding" 但 2.x 实现尚未跟上——文档超前于代码，建议降级 tutorial 描述或补齐 pivot。

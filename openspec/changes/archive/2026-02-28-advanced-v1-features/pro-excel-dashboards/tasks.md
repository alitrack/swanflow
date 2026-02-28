# Tasks: Professional Excel Dashboards

## 1. Multi-Input Excel Sink

- [ ] 1.1 Refactor `ExcelSinkLogical` and `ExcelSinkExec` to support multiple input ports (one port per sheet).
- [ ] 1.2 Update `SwanFlowPlanner` to handle the multi-input mapping for Excel workbooks.
- [ ] 1.3 Add sheet-level naming and ordering to the node configuration.

## 2. Advanced Visuals and Pivots

- [ ] 2.1 Implement `PivotTableSpec` in node configuration: handle rows, columns, and aggregate fields.
- [ ] 2.2 Enhance `ExcelWritingStream` to generate native Excel Pivot Tables using the calculated result batches.
- [ ] 2.3 Implement "Style Registry" support: map JSON style definitions to `rust_xlsxwriter::Format`.

## 3. Template Engine

- [ ] 3.1 Implement `ExcelTemplateNode`: read an existing workbook and identify marker cells (e.g., `{{revenue_table}}`).
- [ ] 3.2 Implement range expansion logic: if a marker is a table, expand the data downwards/sideways.
- [ ] 3.3 Verify template fill with a sample "Executive Summary" template.

## 4. Agent and UX

- [ ] 4.1 Update `AnalystAgent` to recognize professional reporting tasks.
- [ ] 4.2 Add "Chart Recommendation" logic: Agent chooses between Line, Column, or Pie based on the aggregation dimension.
- [ ] 4.3 Create `Tutorial 09: Professional Excel Reports` showing pivots and branding.

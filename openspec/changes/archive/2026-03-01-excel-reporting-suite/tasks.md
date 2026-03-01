## 1. Excel Template Filling (Phase 1)

- [x] 1.1 Implement `ExcelTemplateNode` skeleton in `src/nodes/io.rs`.
- [x] 1.2 Implement template reading logic using `calamine` to find markers.
- [x] 1.3 Implement value replacement and table injection using `rust_xlsxwriter`.
- [x] 1.4 Add unit tests for `ExcelTemplateNode` with a sample template.

## 2. Declarative Styling (Phase 2)

- [x] 2.1 Update `ExcelWriteNode` config schema to support `styles` and `conditional_formatting`.
- [x] 2.2 Implement style mapping logic in `ExcelWritingStream::finalize_workbook`.
- [x] 2.3 Implement Excel Table (ListObject) creation in `ExcelWritingStream`.
- [x] 2.4 Add tests for styled Excel output and Table conversion.

## 3. Automated Reporting & Charts (Phase 3)

- [x] 3.1 Implement `ExcelReportNode` and its configuration schema for programmatic charts.
- [x] 3.2 Implement chart generation logic (Column, Line, Pie) in the physical executor.
- [x] 3.3 Implement `ExcelSheetNamesNode` for workbook introspection.
- [x] 3.4 Add tests for automated chart generation and metadata extraction.

## 4. System Integration & Finalization

- [x] 4.1 Register all new nodes in `src/nodes/registry.rs`.
- [x] 4.2 Update `examples/` and `tutorials/` with new Excel reporting demos.
- [x] 4.3 Verify end-to-end performance and memory usage for large report generation.

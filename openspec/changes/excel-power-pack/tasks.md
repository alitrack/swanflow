# Tasks: Excel Power Pack

## 1. Setup and Infrastructure

- [ ] 1.1 Add `calamine` and `rust_xlsxwriter` to `Cargo.toml`.
- [ ] 1.2 Update `NodeBehavior` trait to support optional `sample_data` return in `infer_schema`.
- [ ] 1.3 Update `SwanFlowEngine::infer_schemas` to return optional sample RecordBatches.

## 2. Excel Reading (Calamine)

- [ ] 2.1 Implement `ExcelReadNode` logic for basic Sheet reading.
- [ ] 2.2 Implement **Type Promotion** logic (merging cell types into Arrow types).
- [ ] 2.3 Implement the **Rejects Port (Port 1)**: capture rows that fail type conversion.
- [ ] 2.4 Implement **Sampling** in `ExcelReadNode::infer_schema` for instant preview.

## 3. Excel Writing (rust_xlsxwriter)

- [ ] 3.1 Implement `ExcelWriteNode` with constant memory writing.
- [ ] 3.2 Implement **Dynamic Input Ports** for multi-sheet writing.
- [ ] 3.3 Add configuration for formatting (e.g. bold headers, column widths).

## 4. Verification and Demos

- [ ] 4.1 Create a demo workflow: `Excel (Dirty) -> SQL Clean -> Excel (Formatted)`.
- [ ] 4.2 Verify that the Rejects port correctly captures illegal rows (e.g. text in a number column).
- [ ] 4.3 Test high-performance reading of a 100k+ row Excel file.

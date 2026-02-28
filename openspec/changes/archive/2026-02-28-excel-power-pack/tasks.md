# Tasks: Excel Power Pack (Deepened)

## 1. Fault Tolerance: The Rejects Port

- [ ] 1.1 Update `ExcelReadNode` to output two separate `LogicalPlan`s (Data and Rejects).
- [ ] 1.2 Implement **Type Promotion Table**: Define rules for `Int` -> `Float` -> `String`.
- [ ] 1.3 Develop a specialized physical operator to route rows based on type-parsing success.
- [ ] 1.4 Add `error_message` column to the Rejects port output for better debugging.

## 2. Dynamic Multi-Sheet Writing

- [ ] 2.1 Refactor `ExcelWriteNode` to support **Dynamic Input Ports**.
- [ ] 2.2 Implement the multiplexing logic in the physical runner to write multiple streams to a single workbook.
- [ ] 2.3 Implement **Auto-fit Columns** logic based on data width.

## 3. Formatting and Styling DSL

- [ ] 3.1 Define a JSON/YAML schema for Excel styling (fonts, colors, borders).
- [ ] 3.2 Map our Style DSL to `rust_xlsxwriter`'s `Format` struct.
- [ ] 3.3 Add configuration for **Header Styling** and **Conditional Formatting** (optional).

## 4. End-to-End Verification

- [ ] 4.1 Demo: Read a "Dirty" Excel -> Clean with SQL -> Write to Multi-Sheet Formatted Excel.
- [ ] 4.2 Verify that the Rejects port correctly captures illegal rows (e.g., text in a price column).
- [ ] 4.3 Integration test for zero-copy memory usage during massive Excel writing.

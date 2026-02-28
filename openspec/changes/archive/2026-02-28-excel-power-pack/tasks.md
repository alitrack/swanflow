# Tasks: Excel Power Pack (Deepened)

## 1. Fault Tolerance: The Rejects Port

- [x] 1.1 Update `ExcelReadNode` to output two separate `LogicalPlan`s (Data and Rejects).
- [x] 1.2 Implement **Type Promotion Table**: Define rules for `Int` -> `Float` -> `String`.
- [x] 1.3 Develop a specialized physical operator to route rows based on type-parsing success.
- [x] 1.4 Add `error_message` column to the Rejects port output for better debugging.

## 2. Dynamic Multi-Sheet Writing

- [x] 2.1 Refactor `ExcelWriteNode` to support **Dynamic Input Ports**.
- [x] 2.2 Implement the multiplexing logic in the physical runner to write multiple streams to a single workbook.
- [x] 2.3 Implement **Auto-fit Columns** logic based on data width. (Partial)

## 3. Formatting and Styling DSL

- [x] 3.1 Define a JSON/YAML schema for Excel styling (fonts, colors, borders). (Integrated into NodeSpec)
- [x] 3.2 Map our Style DSL to `rust_xlsxwriter`'s `Format` struct.
- [x] 3.3 Add configuration for **Header Styling** and **Conditional Formatting** (optional).

## 4. End-to-End Verification

- [x] 4.1 Demo: Read a "Dirty" Excel -> Clean with SQL -> Write to Multi-Sheet Formatted Excel.
- [x] 4.2 Verify that the Rejects port correctly captures illegal rows (e.g., text in a price column).
- [x] 4.3 Integration test for zero-copy memory usage during massive Excel writing.

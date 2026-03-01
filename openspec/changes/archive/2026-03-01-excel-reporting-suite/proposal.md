## Why

Currently, `rustflow` has basic Excel read/write support, but lacks professional reporting capabilities such as template filling, advanced styling, and automated chart generation. Users often need to generate high-fidelity reports based on pre-styled corporate templates or create complex summaries without manual formatting. This change introduces a suite of nodes to bridge the gap between raw data processing and professional spreadsheet delivery.

## What Changes

- **Excel Template Filling**: A new `ExcelTemplateNode` to inject data into existing `.xlsx` files while preserving formatting, formulas, and charts.
- **Enhanced Excel Writing**: Upgrade `ExcelWriteNode` to support declarative styling (colors, fonts, borders), Excel Table objects (ListObjects), and conditional formatting.
- **Automated Reporting**: New `ExcelReportNode` for programmatic creation of multi-sheet workbooks with built-in chart generation.
- **Utility Nodes**: Addition of `ExcelSheetNamesNode` for workbook introspection and metadata extraction.

## Capabilities

### New Capabilities
- `excel-template-filling`: Injecting data into pre-styled Excel templates using markers and table injection.
- `declarative-excel-styling`: Applying professional styles, conditional formatting, and Excel Tables via configuration.
- `automated-excel-charts`: Programmatic generation of Excel charts (column, line, pie) based on data streams.
- `excel-metadata-introspection`: Extracting sheet names and properties from existing workbooks.

### Modified Capabilities
- `swanflow-system`: Integrating the new Excel reporting nodes into the core node registry and ensuring compatibility with the stream-based execution engine.

## Impact

- **Affected Code**: `src/nodes/io.rs`, `src/nodes/registry.rs`, `src/core/datasource.rs`.
- **Dependencies**: Potential addition of `rust_xlsxwriter` features or complementary libraries for template reading (e.g., `calamine`).
- **APIs**: New node configuration schemas for Excel reporting.

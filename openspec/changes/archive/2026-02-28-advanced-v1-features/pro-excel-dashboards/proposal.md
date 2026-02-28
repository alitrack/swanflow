## Why
To enable SwanFlow to produce boardroom-quality business reports. Currently, our Excel output is limited to basic data dumps with simple charts. Business users require professional layouts, custom branding (logo/colors), pivot tables for multi-dimensional analysis, and highly customized charts. This change elevates SwanFlow from a data engine to a complete reporting solution.

## What Changes
- Implement `ExcelTemplateNode`: Allow users to provide an existing `.xlsx` file as a template and fill specific ranges with data.
- Enhance `ExcelSinkExec`: Add support for rich formatting (conditional formatting, borders, fonts) and native Pivot Table generation.
- Implement **Multi-Sheet Reporting**: Support writing different DataFusion result sets to specific sheets within a single workbook.
- Add **Agent Visualization Logic**: Empower the Analyst Agent to choose between different chart types and professional color palettes based on data characteristics.

## Capabilities

### New Capabilities
- `excel-template-engine`: Ability to map DataFusion outputs to specific cells/ranges in a pre-styled Excel template.
- `native-pivot-tables`: Programmatic generation of Excel Pivot Tables from Arrow record batches.
- `report-branding-toolkit`: Configuration options for logos, headers, and professional CSS-like styling for Excel cells.

### Modified Capabilities
- `excel-reporting`: Upgraded from basic data sinking to a high-fidelity dashboard generation capability.

## Impact
- **src/core/datasource.rs**: Extensive updates to `ExcelSinkExec` to support `rust_xlsxwriter` advanced features.
- **src/nodes/io.rs**: Implementation of `ExcelTemplateNode`.
- **Analyst Agent**: New reasoning patterns for "Visual Design" alongside "Data Analysis".

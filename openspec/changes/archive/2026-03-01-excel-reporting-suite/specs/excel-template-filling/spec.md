## ADDED Requirements

### Requirement: Marker-based Cell Injection
The `ExcelTemplateNode` SHALL support injecting single values from the input data into an existing Excel template by replacing markers formatted as `{{FieldName}}`.

#### Scenario: Replace customer name marker
- **WHEN** an Excel template contains `{{CustomerName}}` in cell B2
- **THEN** the system SHALL replace it with the value "Acme Corp" from the input stream.

### Requirement: Table-based Range Injection
The `ExcelTemplateNode` SHALL support injecting an entire DataFusion `RecordBatch` into a specified starting cell (e.g., `A10`) of a template sheet, automatically expanding downwards.

#### Scenario: Inject sales data table
- **WHEN** a user configures a table injection starting at cell `A10` on "Sheet1"
- **THEN** the system SHALL write all rows from the input stream starting at that coordinate, shifting or overwriting existing content based on configuration.

### Requirement: Preservation of Formatting and Objects
The `ExcelTemplateNode` MUST preserve existing styles, formulas, charts, and pivot tables in the template file that are not explicitly overwritten by data injection.

#### Scenario: Update chart via data injection
- **WHEN** a template has a chart pointing to the range `A10:B20`
- **THEN** injecting new data into `A10:B20` SHALL cause the chart to reflect the new values when the file is opened.

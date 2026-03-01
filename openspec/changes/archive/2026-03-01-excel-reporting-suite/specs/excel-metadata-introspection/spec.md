## ADDED Requirements

### Requirement: Get Sheet Names
The `ExcelSheetNamesNode` SHALL take an Excel file path as input and output a DataFusion `RecordBatch` containing all worksheet names in the workbook.

#### Scenario: Retrieve sheet list
- **WHEN** an input path points to a file with sheets "Sheet1", "Dashboard", and "Data"
- **THEN** the system SHALL output a single-column table with those three names.

### Requirement: Sheet Existence Check
The `ExcelSheetNamesNode` SHALL allow users to check if a specific sheet name exists within the workbook.

#### Scenario: Verify sheet exists
- **WHEN** searching for "Sales" in a file with "Sheet1", "Sales", "Data"
- **THEN** the system SHALL return a boolean or a filtered record batch confirming its presence.

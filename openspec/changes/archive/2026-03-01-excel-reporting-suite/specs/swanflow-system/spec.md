## ADDED Requirements

### Requirement: Excel Node Registration
The `rustflow` registry SHALL include the `ExcelTemplateNode`, `ExcelReportNode`, and `ExcelSheetNamesNode` to make them available for workflow construction.

#### Scenario: Verify node registration
- **WHEN** the engine initializes
- **THEN** the `ExcelTemplateNode` and `ExcelReportNode` SHALL be present in the node registry.

### Requirement: Stream-based Professional Writing
All new Excel writing nodes MUST integrate with the `ExcelWritingStream` or a similar stream-based physical operator to ensure zero-copy memory management and efficient disk IO.

#### Scenario: Efficient stream-based report generation
- **WHEN** a workflow generates a 10,000-row report
- **THEN** the system SHALL stream those rows to the Excel file without buffering the entire dataset in memory.

## ADDED Requirements

### Requirement: Programmatic Column and Line Charts
The `ExcelReportNode` SHALL support programmatic creation of column, line, and pie charts based on data from a specific input stream and worksheet range.

#### Scenario: Add column chart to worksheet
- **WHEN** a chart config defines a Column chart with data range `Sheet1!$B$2:$B$11` and labels `Sheet1!$A$2:$A$11`
- **THEN** the output Excel workbook SHALL contain a column chart object on that sheet.

### Requirement: Chart Title and Axis Labels
The `ExcelReportNode` SHALL allow users to specify chart metadata including title, x-axis label, and y-axis label via node configuration.

#### Scenario: Set chart title to "Annual Revenue"
- **WHEN** the chart configuration specifies `title: "Annual Revenue"`
- **THEN** the chart object in the final Excel file SHALL display that title.

### Requirement: Chart Placement and Sizing
The `ExcelReportNode` SHALL support specifying the top-left cell coordinate and the dimensions (width, height) for chart objects.

#### Scenario: Place chart at cell D2
- **WHEN** a chart's `offset` is set to `{"row": 1, "col": 3}`
- **THEN** the chart SHALL be anchored at cell `D2` in the worksheet.

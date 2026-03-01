## ADDED Requirements

### Requirement: Declarative Column Styling
The `ExcelWriteNode` SHALL allow users to define a JSON style configuration that maps column names to specific `xlsxwriter` format properties (e.g., `bold`, `font_color`, `bg_color`).

#### Scenario: Apply bold and blue to Header
- **WHEN** a user defines a style for the "Revenue" column with `{"font_color": "blue", "bold": true}`
- **THEN** all values in that column SHALL be formatted accordingly in the output Excel file.

### Requirement: Excel Table (ListObject) Support
The `ExcelWriteNode` SHALL provide an option to automatically convert the written range into an official Excel `Table` object with integrated filtering and zebra-striping.

#### Scenario: Convert output to Table
- **WHEN** the `convert_to_table` flag is set to true in the node configuration
- **THEN** the output range on the worksheet SHALL be defined as an Excel Table named "Table1".

### Requirement: Conditional Cell Formatting
The `ExcelWriteNode` SHALL support declarative conditional formatting rules (e.g., `value < 0` renders text in red).

#### Scenario: Highlight negative values
- **WHEN** a conditional rule `{"condition": "less_than", "value": 0, "format": {"font_color": "red"}}` is applied to the "Profit" column
- **THEN** any cell in that column with a negative value SHALL be rendered in red.

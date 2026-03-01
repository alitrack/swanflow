## Context

`rustflow` currently uses `calamine` for reading Excel and `rust_xlsxwriter` for writing. While these are powerful, they don't natively support "editing" existing files in a single pass. The current `ExcelWriteNode` is a simple sink that creates a new workbook from scratch. We need a strategy to handle template-based reporting where pre-existing styles and charts must be preserved.

## Goals / Non-Goals

**Goals:**
- **Template Preservation**: Allow users to fill data into a `.xlsx` file without losing existing charts, pivot tables, or macro-enabled content.
- **Declarative Formatting**: Enable advanced cell styling (colors, borders, fonts) via node configuration.
- **Scalability**: Maintain the current stream-based execution model where data is written as it's processed.
- **Extensibility**: Provide a foundation for adding more complex Excel features like Pivot Table generation in the future.

**Non-Goals:**
- **In-place Editing**: We will NOT modify files in-place. We will follow a "Read-Fill-SaveAs" pattern for safety.
- **Full Excel Formula Engine**: We will not implement a formula engine in Rust; we will rely on Excel's own recalculation when the file is opened.

## Decisions

### 1. Template Filling Strategy (Read-Fill-Save)
**Rationale**: `rust_xlsxwriter` is strictly a writer. To support templates, we will use a hybrid approach:
- Use `calamine` to read the template structure and metadata.
- Use `rust_xlsxwriter` to recreate the workbook, copying styles and static content from the template, then injecting new data.
- **Alternative**: Using a C-based library like `libxlsxwriter` via FFI. Rejected due to safety and compilation complexity in Rust.

### 2. Marker-based Injection for `ExcelTemplateNode`
**Rationale**: Users will specify markers like `{{CustomerName}}` in cells. The node will:
- Scan the template for these markers.
- Map them to input data fields.
- Replace markers with actual values during the write phase.
- **Alternative**: Coordinate-based injection (e.g., "Fill B2"). We will support BOTH (Coordinates for tables, Markers for single cells).

### 3. Declarative Styling for `ExcelWriteNode`
**Rationale**: Instead of imperative code, the node configuration will include a `styles` array that maps column names or conditions to `rust_xlsxwriter::Format` objects.
- **Example**: `{"column": "Revenue", "style": {"font_color": "green", "bold": true}}`

## Risks / Trade-offs

- **[Risk] Complex Template Corruption** → **Mitigation**: Start with simple template support (data and static cells) and explicitly document limitations regarding complex objects like embedded OLE objects or specialized VBA projects that might not survive the "Read-Fill-Save" cycle.
- **[Risk] Memory Usage for Large Templates** → **Mitigation**: Use stream-based writing where possible, although template reading typically requires loading the structure into memory.

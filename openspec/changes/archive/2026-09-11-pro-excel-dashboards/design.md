## Context
SwanFlow needs to move from "data export" to "report generation". This requires a deeper integration with `rust_xlsxwriter` and a new logical node that can handle templates. Professional reports often require a mix of raw data, summary tables (Pivot), and visualizations, all conforming to a specific layout.

## Goals / Non-Goals

**Goals:**
- Implement `ExcelTemplateNode`: Map data ports to specific cell addresses (e.g., `A5`, `Sheet2!B10`).
- Implement native Pivot Table support: Define rows, columns, and values in node config.
- Support rich formatting: Define "Style Profiles" (JSON) that map to Excel formats.
- Enable multi-input Excel sinking: One node, multiple input ports, one workbook.

**Non-Goals:**
- Excel macro (VBA) injection or execution.
- Real-time Excel co-authoring integration.

## Decisions

### 1. Multi-Input Sink Architecture
**Decision**: Refactor `ExcelSinkLogical` to accept `Vec<LogicalPlan>` instead of a single input.
**Rationale**: In professional reports, we often want to write multiple tables (e.g., "Summary" and "Details") to different sheets in one go. A multi-input sink is more efficient and semantically correct for a "Workbook".

### 2. Style-First Configuration
**Decision**: Use a "Style Registry" concept within the node configuration.
**Rationale**: Instead of repeating "Bold, Blue, 12pt" for every column, the Agent can define a `header_style` once and apply it to all headers. This is cleaner and easier for an LLM to generate.

### 3. Pivot Table Automation
**Decision**: Leverage `rust_xlsxwriter`'s Pivot Table API.
**Rationale**: Doing pivots in DataFusion is great for calculation, but creating a *native* Excel Pivot Table allows the end user to interact with the data (filter/sort) inside Excel. We will calculate the data in DataFusion but wrap it in a native Pivot object.

## Risks / Trade-offs
- **[Risk] Template Complexity** → Mitigation: `ExcelTemplateNode` will support simple "Variable Search and Replace" in cells first, before supporting complex range expansion.
- **[Risk] Memory Usage** → Mitigation: `rust_xlsxwriter` is designed for constant-memory writing, but building large workbooks with multiple sheets requires careful buffering of batches. We will collect batches per sheet before final workbook finalization.

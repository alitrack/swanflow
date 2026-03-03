## ADDED Requirements

### Requirement: MCP Output Normalization
The system SHALL provide an `McpInvokeNode` that captures the results of a tool call and converts them into a standardized Arrow RecordBatch.

#### Scenario: Text content conversion
- **WHEN** a tool returns a result with multiple text content blocks
- **THEN** the system SHALL output a RecordBatch with a column `content` (Utf8) where each row corresponds to a content block.

### Requirement: Metadata Enrichment
The `McpInvokeNode` SHALL include metadata such as `tool_name` and `is_error` in the output RecordBatch to allow downstream logic to handle tool failures.

#### Scenario: Tool error reporting
- **WHEN** a tool call returns an error status
- **THEN** the output RecordBatch SHALL have the `is_error` flag set to `true` for those rows.

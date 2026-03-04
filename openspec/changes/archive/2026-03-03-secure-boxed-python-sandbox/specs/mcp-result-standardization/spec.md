## MODIFIED Requirements

### Requirement: MCP Output Normalization
The system SHALL provide an `McpInvokeNode` that captures the results of a tool call and converts them into a standardized Arrow RecordBatch, with enhanced support for multi-part sandboxed execution results.

#### Scenario: Capturing logs and data
- **WHEN** a `BoxedPythonNode` completes
- **THEN** the system SHALL capture the `stdout` as a log column and the `results.parquet` link as the primary data output.

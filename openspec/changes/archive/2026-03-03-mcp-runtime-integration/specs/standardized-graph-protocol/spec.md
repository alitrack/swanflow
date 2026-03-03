## ADDED Requirements

### Requirement: MCP-Specific Validation Support
The `TableSchemaValidator` SHALL be enhanced to handle raw text content from MCP output, attempting to parse it as structured graph data if appropriate.

#### Scenario: Validating tool-returned JSON
- **WHEN** the input to the validator comes from an `McpInvokeNode`
- **THEN** the validator SHALL inspect the `content` column and attempt to extract entities and edges from any embedded JSON objects.

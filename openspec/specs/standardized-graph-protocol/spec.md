## ADDED Requirements

### Requirement: Golden Graph Schema Enforcement
The system SHALL strictly enforce a standardized "Golden Graph Schema" for all nodes producing or consuming graph data:
- **Node Table**: `id (Int64)`, `label (Utf8)`, `properties (JSON/Utf8)`
- **Edge Table**: `source_id (Int64)`, `target_id (Int64)`, `type (Utf8)`, `weight (Float64)`

#### Scenario: Graph schema validation
- **WHEN** a node outputs a RecordBatch to a Graph port
- **THEN** it SHALL be rejected if the schema does not match the Golden Schema exactly.

### Requirement: Automated Schema Validator (Mapping)
The system SHALL provide a `TableSchemaValidator` node that uses an LLM to map arbitrary input table schemas (from CSV, JSON, or Agents) to the Golden Graph Schema.

#### Scenario: Mapping agent output to graph schema
- **WHEN** a research agent output contains columns `person_name`, `works_at`, `job_title`
- **THEN** the `TableSchemaValidator` SHALL automatically map `person_name` to `label` (Node table) and `works_at` to `type` (Edge table).
## ADDED Requirements

### Requirement: MCP-Specific Validation Support
The `TableSchemaValidator` SHALL be enhanced to handle raw text content from MCP output, attempting to parse it as structured graph data if appropriate.

#### Scenario: Validating tool-returned JSON
- **WHEN** the input to the validator comes from an `McpInvokeNode`
- **THEN** the validator SHALL inspect the `content` column and attempt to extract entities and edges from any embedded JSON objects.

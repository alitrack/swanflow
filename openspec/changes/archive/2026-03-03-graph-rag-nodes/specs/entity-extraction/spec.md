## ADDED Requirements

### Requirement: LLM-Based Entity Discovery
The system SHALL utilize an LLM to identify unique entities (nodes) within a given text chunk, categorizing them by labels (e.g., PERSON, ORG, CONCEPT).

#### Scenario: Successful entity extraction
- **WHEN** a text chunk describing "Alice works for ACME Corp" is processed
- **THEN** the system SHALL output two rows in the Node port: "Alice" (label: PERSON) and "ACME Corp" (label: ORG).

### Requirement: Relationship Extraction
The system SHALL identify semantic relationships (edges) between discovered entities, assigning a relationship type and weight.

#### Scenario: Successful relationship extraction
- **WHEN** the text chunk "Alice works for ACME Corp" is processed
- **THEN** the system SHALL output one row in the Edge port connecting "Alice" to "ACME Corp" with type "works_for".

### Requirement: Consistent Graph Schema
The system SHALL output extracted graph data in a standardized Arrow RecordBatch format for nodes and edges.

#### Scenario: Schema validation
- **WHEN** the extraction node completes its task
- **THEN** the Node port RecordBatch SHALL contain `id`, `label`, and `properties` columns
- **AND** the Edge port RecordBatch SHALL contain `source_id`, `target_id`, `type`, and `weight` columns.

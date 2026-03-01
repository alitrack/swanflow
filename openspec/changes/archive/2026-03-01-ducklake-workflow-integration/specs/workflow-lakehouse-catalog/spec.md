## ADDED Requirements

### Requirement: Automatic Table Registration
The system SHALL automatically register every materialized node as a table in the DuckLake catalog using the format `node_<uuid_prefix>`.

#### Scenario: Register node after execution
- **WHEN** a node with ID `e1f2a3b4...` finishes materialization
- **THEN** a table named `node_e1f2a3b4` SHALL be available in the DataFusion catalog.

### Requirement: DuckDB Metadata Backend
The catalog SHALL use a local DuckDB file to store table definitions, schemas, and snapshot metadata.

#### Scenario: Persistence across sessions
- **WHEN** the workflow engine is restarted
- **THEN** it SHALL be able to reload the previous node states from the DuckDB-backed catalog.

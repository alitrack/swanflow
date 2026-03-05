# m0-agent-node-gap-closure Specification

## Purpose
TBD - created by archiving change m0-agent-node-gap-closure. Update Purpose after archive.
## Requirements
### Requirement: Shared AI Contracts
The system SHALL define reusable, serializable AI contract structs for citation and structured output nodes.

#### Scenario: Citation contract serialization
- **WHEN** a citation item is created with `source_id`, `snippet`, `score`, and span offsets
- **THEN** it SHALL serialize to JSON without error
- **AND** field names SHALL be stable for downstream nodes.

### Requirement: M0 Node Registration and Pipeline Compilation
The system SHALL register M0 node-gap closure operators in the core node registry so that end-to-end compilation succeeds.

#### Scenario: Basic M0 RAG chain compiles
- **WHEN** a workflow chains query planning, semantic chunking, vector writer/retriever, structured IO contract, and citation tracker
- **THEN** workflow compilation SHALL succeed with the default registry.

#### Scenario: Tool-assisted M0 chain compiles
- **WHEN** a workflow chains tool executor, structured IO contract, and citation tracker
- **THEN** workflow compilation SHALL succeed with the default registry.

### Requirement: Universal Resolver Routing Contract
The universal resolver SHALL expose deterministic route metadata for `url`, `file`, and `table` source routing.

#### Scenario: Auto and explicit source routing
- **WHEN** `source` and `source_type` are provided for URL, file path, and table query inputs
- **THEN** route metadata SHALL include `resolved_source_type`
- **AND** the value SHALL match the expected route category.


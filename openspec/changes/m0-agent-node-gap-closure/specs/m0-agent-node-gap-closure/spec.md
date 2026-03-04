## ADDED Requirements

### Requirement: Shared AI Contracts
The system SHALL define reusable, serializable AI contract structs for citation and structured output nodes.

#### Scenario: Citation contract serialization
- **WHEN** a citation item is created with `source_id`, `snippet`, `score`, and span offsets
- **THEN** it SHALL serialize to JSON without error
- **AND** field names SHALL be stable for downstream nodes.

## ADDED Requirements

### Requirement: Multi-Hop Neighbor Search
The system SHALL retrieve the immediate neighborhood (nodes and edges) of entities identified in a user query, supporting configurable traversal depth (e.g., 1-hop, 2-hop).

#### Scenario: 1-hop neighbor retrieval
- **WHEN** a query mentions "Alice"
- **THEN** the system SHALL retrieve "Alice" and all entities directly connected to her (e.g., "ACME Corp") from the graph index.

### Requirement: Relationship-Aware Context Generation
The system SHALL format retrieved graph fragments into a textual context suitable for LLM consumption, explicitly including relationship types.

#### Scenario: Path-based context construction
- **WHEN** a path "Alice --[works_for]--> ACME Corp" is retrieved
- **THEN** the system SHALL generate a context string like "Alice works for ACME Corp" or a structured equivalent.

### Requirement: Keyword-to-Entity Mapping
The system SHALL attempt to map user-provided query keywords to indexed entity IDs using exact or fuzzy matching.

#### Scenario: Successful entity resolution
- **WHEN** a user asks about "ACME"
- **THEN** the system SHALL resolve this to the indexed entity "ACME Corp" and initiate a graph traversal from that node.

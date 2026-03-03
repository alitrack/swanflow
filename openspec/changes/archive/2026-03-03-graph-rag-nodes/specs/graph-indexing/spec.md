## ADDED Requirements

### Requirement: Persistent DuckDB Graph Storage
The system SHALL store nodes and edges in dedicated system-level DuckDB tables (`__graph_nodes` and `__graph_edges`) within the workflow's `cache_dir`.

#### Scenario: Initial graph creation
- **WHEN** the `graph_index_writer` node is executed for the first time
- **THEN** the system SHALL create the `__graph_nodes` and `__graph_edges` tables if they do not exist.

### Requirement: Idempotent Incremental Indexing
The system SHALL support idempotent updates to the graph index, merging new nodes and edges with existing ones without creating duplicate records.

#### Scenario: Incremental node update
- **WHEN** a node with an existing `id` is processed by the indexer
- **THEN** the system SHALL update the existing node's properties instead of inserting a new row.

### Requirement: Transactional Atomicity
The system SHALL ensure that all updates to the graph index within a single workflow stage are committed atomically.

#### Scenario: Atomic commit
- **WHEN** multiple nodes are processed in one indexing stage
- **THEN** either all nodes and edges are successfully indexed, or none are, ensuring the graph remains in a consistent state.

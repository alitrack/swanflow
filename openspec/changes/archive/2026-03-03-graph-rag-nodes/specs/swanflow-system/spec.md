## ADDED Requirements

### Requirement: Table-as-Graph Data Protocol
The system SHALL support a standardized "Table-as-Graph" protocol where property graphs are exchanged between nodes as pairs of Arrow RecordBatches.

#### Scenario: Node and Edge port compliance
- **WHEN** a node outputs graph data
- **THEN** it SHALL provide a Node port with a schema including `id: Int64`, `label: Utf8`, and `properties: Utf8 (JSON)`
- **AND** an Edge port with a schema including `source_id: Int64`, `target_id: Int64`, `type: Utf8`, and `weight: Float64`.

### Requirement: Cross-Node Graph Interoperability
The system SHALL allow any node that accepts Arrow RecordBatches to process Node or Edge tables, facilitating the use of relational operators (e.g., SQL JOIN) on graph data.

#### Scenario: Relational filtering of graph data
- **WHEN** an `Edges` RecordBatch is connected to a `SqlNode`
- **THEN** the user SHALL be able to execute relational queries (e.g., `SELECT * FROM input_0 WHERE weight > 0.5`) on the edge data.

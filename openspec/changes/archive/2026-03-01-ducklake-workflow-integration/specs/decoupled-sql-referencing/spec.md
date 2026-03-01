## ADDED Requirements

### Requirement: Cross-node SQL Access
The `SqlNode` SHALL allow users to query any materialized node in the workflow using its catalog table name, even if no direct edge exists between them.

#### Scenario: Query distant node
- **WHEN** a user writes `SELECT * FROM node_e1f2a3b4` in a `SqlNode`
- **THEN** the engine SHALL resolve this to the DuckLake table representing that specific node's output.

### Requirement: Automatic Dependency Detection
The compiler SHALL automatically detect SQL references to other nodes and ensure they are executed in the correct order.

#### Scenario: Implicit dependency
- **WHEN** Node B's SQL references Node A's table name
- **THEN** the compiler SHALL ensure Node A executes and materializes its data before Node B starts.

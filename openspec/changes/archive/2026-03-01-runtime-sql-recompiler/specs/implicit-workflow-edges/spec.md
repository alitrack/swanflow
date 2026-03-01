## ADDED Requirements

### Requirement: SQL-based Dependency Discovery
The `WorkflowCompiler` SHALL scan SQL query strings for `node_<uuid>` patterns to determine implicit execution dependencies.

#### Scenario: Add implicit edge
- **WHEN** Node B's SQL query contains the string `node_abc123`
- **THEN** the compiler SHALL add an implicit edge from Node A (the producer of `node_abc123`) to Node B.

### Requirement: Topological Ordering of Implicit Dependencies
Implicit dependencies SHALL be treated with the same priority as explicit edges when calculating the workflow execution order.

#### Scenario: Correct execution order
- **WHEN** Node B implicitly depends on Node A via SQL
- **THEN** Node A SHALL always be scheduled for execution before Node B.

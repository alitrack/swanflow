## ADDED Requirements

### Requirement: Deferred SQL Parsing
The `SqlNode` SHALL NOT parse its SQL query during the `propose_plan` phase but instead return a proxy logical node.

#### Scenario: Successful compilation with missing table
- **WHEN** a `SqlNode` references a table that does not yet exist in the catalog
- **THEN** the workflow compilation SHALL succeed by returning a `SqlProxyLogical`.

### Requirement: Runtime Physical Realization
The `SwanFlowPlanner` SHALL realize `SqlProxyLogical` into a physical `ExecutionPlan` by calling `SessionContext::sql()` during the physical planning phase.

#### Scenario: Execute decoupled SQL
- **WHEN** a `SqlProxyLogical` is encountered in the physical planner and its referenced table has been materialized
- **THEN** the planner SHALL successfully generate and return the corresponding physical plan.

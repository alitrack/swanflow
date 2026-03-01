## MODIFIED Requirements

### Requirement: Execution Plan Node Interface
The `SwanFlowPlanner` SHALL support the realization of `SqlProxyLogical` nodes using the global `SessionContext`.

#### Scenario: Planner context access
- **WHEN** the physical planner Realizes a `SqlProxyLogical`
- **THEN** it SHALL have read/write access to the current `SessionContext` to register inputs and parse the query.

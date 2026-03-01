## ADDED Requirements

### Requirement: Efficient Cycle Detection
The `Workflow` implementation SHALL provide an efficient mechanism for detecting cycles when adding edges, with a complexity not exceeding $O(N + E)$, and without cloning the entire graph for each addition.

#### Scenario: Cycle detection on edge addition
- **WHEN** an edge is added that would create a directed cycle in the workflow graph
- **THEN** the addition SHALL fail with an appropriate error message and the graph MUST remain unchanged.

### Requirement: Optimized Workflow Compilation
The `WorkflowCompiler` SHALL perform compilation of a logical workflow into a DataFusion plan with efficient topological sorting and variable-dependency detection ($O(N+E)$ complexity).

#### Scenario: Variable dependency ordering
- **WHEN** a node `B` depends on a variable `${context.x}` produced by node `A`
- **THEN** the compiler SHALL ensure node `A` is compiled and its variables updated before node `B`.

### Requirement: Node Event Error Reporting
The `NodeEvent` stream SHALL accurately report errors encountered during the physical execution of each node's plan, including specific error messages from the DataFusion or Python engines.

#### Scenario: Execution error broadcast
- **WHEN** a `SqlNode` fails due to an invalid query during physical execution
- **THEN** a `NodeEvent::Error` SHALL be broadcast with the specific error message and node ID.

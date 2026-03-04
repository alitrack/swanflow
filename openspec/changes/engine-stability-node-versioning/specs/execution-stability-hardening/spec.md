## ADDED Requirements

### Requirement: Mandatory Preflight Validation Gate
The compiler SHALL run a mandatory preflight for every node before planning/execution, including config migration and config validation. If preflight fails, planning for that node MUST stop and return a structured error without panic.

#### Scenario: Invalid node configuration blocks planning
- **WHEN** a workflow contains a node with missing required config fields
- **THEN** the compiler SHALL fail that node in preflight with a structured validation error
- **AND** the engine SHALL emit an error event containing node id, node type, and failure class
- **AND** the process MUST NOT panic.

### Requirement: Panic-Free Config Access in Critical Nodes
Critical planning/runtime nodes (`core.io.*`, `core.transform.*`, `core.sql`, `core.script.*`) SHALL use typed config extraction and SHALL NOT rely on panic-based access for required config keys.

#### Scenario: Missing required key in core.io node
- **WHEN** a `core.io.csv` node is configured without `path`
- **THEN** planning SHALL return a deterministic config error for that node
- **AND** workflow execution MUST continue to error propagation without process crash.

### Requirement: Deterministic Error Classification
Preflight and execution failures SHALL be classified into deterministic classes (`validation`, `migration`, `planning`, `execution`) for downstream policy handling.

#### Scenario: Classified SQL planning failure
- **WHEN** a `core.sql` node fails planning due to unresolved table reference
- **THEN** the error SHALL be emitted as class `planning`
- **AND** self-healing policy evaluation SHALL receive this class as input.

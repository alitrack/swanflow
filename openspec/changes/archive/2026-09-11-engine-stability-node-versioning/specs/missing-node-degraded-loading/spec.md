## ADDED Requirements

### Requirement: Placeholder Node for Unknown Types
The loader SHALL replace unknown or unavailable `type_id` values with a placeholder node that preserves original node metadata and raw config.

#### Scenario: Unknown node type during workflow load
- **WHEN** a workflow references a node type not found in the registry
- **THEN** the workflow SHALL still load with a placeholder node at the same node id
- **AND** the placeholder SHALL retain the original `type_id` and config payload.

### Requirement: Inspectable Degraded Workflow
Loaded workflows containing placeholder nodes SHALL remain inspectable for metadata, schema inference boundaries, and dependency graph diagnostics.

#### Scenario: Inspect command with placeholder node
- **WHEN** a user runs workflow inspect on a graph containing placeholder nodes
- **THEN** the inspect output SHALL identify placeholder nodes and original type ids
- **AND** the inspect command SHALL complete without panic.

### Requirement: Non-Executable Placeholder Behavior
Placeholder nodes SHALL be non-executable and SHALL emit an explicit remediation error when execution reaches them.

#### Scenario: Run workflow containing placeholder node
- **WHEN** workflow execution reaches a placeholder node
- **THEN** the engine SHALL emit a structured error indicating missing node implementation
- **AND** the error SHALL include remediation guidance (install extension or migrate node type)
- **AND** execution MUST NOT panic.

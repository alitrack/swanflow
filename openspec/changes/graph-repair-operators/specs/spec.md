# Spec: Graph Repair Operators

## ADDED Requirements

### Requirement: Five-Operator Repair

The system SHALL provide a typed repair operator set, `REBIND | INSERT_PREREQ | SUBSTITUTE | REWIRE | BYPASS`, selectable by the LLM repair agent based on the classified failure type.

#### Scenario: Config error repair (REBIND)
- **WHEN** a node fails with a parameter error (invalid JSON config, wrong value)
- **THEN** the repair agent SHALL respond with `{"operator":"REBIND","fixed_config":{...}}`
- **AND** the engine SHALL update the failed node's config and re-execute it.

#### Scenario: Missing precondition repair (INSERT_PREREQ)
- **WHEN** a node fails because a required upstream artifact does not exist (e.g., SQL references a missing column/table)
- **THEN** the repair agent SHALL respond with `{"operator":"INSERT_PREREQ","insert_node":{...},"insert_before":"<failed-node-id>"}`
- **AND** the engine SHALL insert the new node before the failed node, wire edges, and re-execute the affected subgraph.

#### Scenario: Wrong node type repair (SUBSTITUTE)
- **WHEN** a node executes successfully but its type is unsuitable for the task
- **THEN** the repair agent SHALL respond with `{"operator":"SUBSTITUTE","new_type":"...","new_config":{...}}`
- **AND** the engine SHALL replace the node type, verifying downstream edge compatibility.

#### Scenario: Dependency rewiring (REWIRE)
- **WHEN** a failure is caused by an incorrect dependency relation in the failed node's neighborhood
- **THEN** the repair agent SHALL respond with `{"operator":"REWIRE","add_edges":[...],"remove_edges":[...]}`
- **AND** the engine SHALL apply the edge changes within the repair neighborhood.

#### Scenario: Skip satisfied node (BYPASS)
- **WHEN** the current state already satisfies the failed node's downstream requirements
- **THEN** the repair agent SHALL respond with `{"operator":"BYPASS"}`
- **AND** the engine SHALL mark the node skipped and continue with its downstream consumers.

### Requirement: Bounded Repair Scope

Repair SHALL be bounded to an h-hop neighborhood of the failed node with patch limits: `h = 2`, `|ΔV| ≤ 3`, `|ΔE| ≤ 5`.

#### Scenario: Localized failure propagation
- **WHEN** a repair is applied
- **THEN** only nodes in the affected subgraph SHALL be re-executed
- **AND** verified ancestors and unaffected branches SHALL NOT be re-executed.

### Requirement: DAG Validity Preservation

Every repair SHALL preserve graph validity: acyclicity, reachability from source to sink, and executability of all nodes.

#### Scenario: Invalid repair rejected
- **WHEN** applying a repair would create a cycle or orphan a node
- **THEN** the engine SHALL reject the repair
- **AND** SHALL escalate to the existing human-in-the-loop path (self-healing-execution).

## MODIFIED Requirements

### Requirement: LLM Repair Agent Output

`OpenAiHook::try_fix` SHALL return a `RepairOperator` instead of a bare config value. The fix prompt SHALL include the full graph topology and operator selection rules. Responses in the legacy format (`{"operator":"REBIND","fixed_config":...}` or bare `{"fixed_config":...}`) SHALL remain parseable.

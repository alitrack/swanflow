## ADDED Requirements

### Requirement: Versioned Node Configuration Metadata
Every node configuration SHALL carry explicit version metadata (`impl_version`, `config_version`) or be deterministically defaulted during load for legacy workflows.

#### Scenario: Legacy workflow without version metadata
- **WHEN** a workflow node config does not include `config_version`
- **THEN** the loader SHALL assign a documented default version
- **AND** the migrator SHALL treat the node as legacy input for upgrade.

### Requirement: Deterministic Migration Pipeline
Node configuration migration SHALL run as an ordered, deterministic pipeline from source `config_version` to target version, with no skipped intermediate transformations unless explicitly marked safe.

#### Scenario: Multi-step migration from v1 to v3
- **WHEN** a node config is at `config_version=1` and target is `3`
- **THEN** the system SHALL run migration steps `1->2` then `2->3` in order
- **AND** resulting config SHALL validate successfully before planning.

### Requirement: Migration Failure Transparency
If migration fails, the engine SHALL expose the exact node id, source/target versions, and migration step that failed.

#### Scenario: Invalid legacy field type during migration
- **WHEN** migration step `2->3` cannot parse a legacy field
- **THEN** the error SHALL include node id, node type, source version `2`, target version `3`, and failing field
- **AND** the node SHALL be rejected before planning.

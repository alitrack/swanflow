## ADDED Requirements

### Requirement: Transactional Snapshot Commits
The `Materializer` SHALL commit data to DuckLake using snapshots to ensure that partial writes are never visible to downstream nodes.

#### Scenario: Atomic write on success
- **WHEN** a materialization stream completes successfully
- **THEN** a new snapshot SHALL be committed to the catalog, making the data atomically visible.

### Requirement: Schema Evolution Support
The system SHALL handle changes in node configuration that result in schema modifications by registering them as new versions in the DuckLake catalog.

#### Scenario: Change group-by columns
- **WHEN** an Aggregate node is modified to include a new group-by column
- **THEN** the DuckLake catalog SHALL update the table schema accordingly while maintaining the integrity of previous snapshots.

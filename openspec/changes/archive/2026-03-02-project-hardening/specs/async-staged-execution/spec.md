## ADDED Requirements

### Requirement: Level-by-Level Execution
The system SHALL execute workflow nodes in topological stages, ensuring all nodes at a given level are completed before nodes at the next level are planned.

#### Scenario: Multi-level materialization
- **WHEN** node A (level 0) materializes a table
- **AND** node B (level 1) queries that table
- **THEN** node B SHALL only be planned and executed after node A's materialization is fully registered.

### Requirement: Non-Blocking Async Materialization
The system SHALL perform all materialization IO (writing Parquet files and updating metadata) asynchronously, without blocking the executor's worker threads.

#### Scenario: Concurrent materialization
- **WHEN** multiple nodes in the same stage finish execution
- **THEN** their materialization logic SHALL run concurrently using non-blocking async IO.

### Requirement: Idempotent Catalog Synchronization
The system SHALL provide a thread-safe and idempotent mechanism to synchronize materialized tables with the `SessionContext`, handling potential duplicate registrations gracefully.

#### Scenario: Duplicate table registration
- **WHEN** a table is already registered in the catalog
- **THEN** the synchronization logic SHALL deregister the old entry before registering the new one, ensuring the latest data is visible.

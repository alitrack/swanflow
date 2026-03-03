## ADDED Requirements

### Requirement: Shared Engine State
The system SHALL maintain a shared, thread-safe execution state using `Arc<SessionContext>` to ensure all components (Compiler, Engine, and Custom Operators) have a consistent view of the DataFusion catalog and configuration.

#### Scenario: Catalog consistency across tasks
- **WHEN** a custom operator registers a new table in the `SessionContext` within a background task
- **THEN** that table SHALL be immediately visible to the main engine thread and subsequent workflow stages without manual synchronization loops.

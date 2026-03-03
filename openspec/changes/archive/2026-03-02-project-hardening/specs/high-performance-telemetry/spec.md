## ADDED Requirements

### Requirement: Buffered Event Reporting
The system SHALL use buffered or throttled communication for node events (e.g., state changes, progress updates) to prevent high-throughput data processing from overwhelming the event receiver.

#### Scenario: High-throughput node events
- **WHEN** a node processes millions of small batches
- **THEN** the system SHALL aggregate or throttle `StateChanged` events to maintain executor performance and receiver stability.

### Requirement: Non-Blocking Telemetry
The system SHALL ensure that generating and sending telemetry data does not introduce blocking delays or significant overhead into the primary data execution path.

#### Scenario: Efficient status update
- **WHEN** a state probe captures a batch
- **THEN** it SHALL emit a status event without waiting for the receiver to process it, utilizing an unbounded or sufficiently large async channel.

## ADDED Requirements

### Requirement: Feedback-Driven MetaNode Execution
The `ResearchLoopMetaNode` SHALL be capable of re-executing its internal sub-graph multiple times within a single workflow run, using the outputs of the previous iteration (extracted clues) as inputs for the next iteration (new search queries).

#### Scenario: Multi-level research cycle
- **WHEN** the initial extraction phase identifies a new entity "Rust-Datafusion"
- **AND** the information gap analysis determines this entity needs further research
- **THEN** the system SHALL automatically trigger a new search for "Rust-Datafusion" without manual user intervention.

### Requirement: Research State Tracking
The system SHALL maintain a state within the `ResearchLoopMetaNode` that tracks "visited" entities, URLs, and queries to prevent redundant operations and infinite loops.

#### Scenario: Preventing circular research
- **WHEN** a search for "Topic A" leads to "Topic B", and "Topic B" leads back to "Topic A"
- **THEN** the system SHALL recognize "Topic A" as already visited and terminate that research branch.

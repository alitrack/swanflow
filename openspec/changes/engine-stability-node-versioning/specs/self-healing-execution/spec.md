## MODIFIED Requirements

### Requirement: Automatic Error Capture and LLM Repair
The system SHALL monitor workflow node execution and SHALL invoke the LLM-based repair agent only for errors classified as recoverable after config migration and validation have completed successfully for that node.

#### Scenario: SQL Syntax Error Recovery
- **WHEN** a `core.sql` node fails due to a recoverable SQL syntax error
- **THEN** the engine SHALL capture the classified error, offending SQL, and node metadata
- **AND** it SHALL call the AI repair agent to generate a corrected SQL query
- **AND** it SHALL attempt to re-execute the node with the new query exactly once.

#### Scenario: Validation or migration failure does not trigger repair
- **WHEN** a node fails in config validation or migration preflight
- **THEN** the system SHALL NOT invoke the AI repair agent
- **AND** it SHALL emit a deterministic preflight error for human/operator resolution.

## ADDED Requirements

### Requirement: Automatic Error Capture and LLM Repair
The system SHALL monitor the execution of all workflow nodes and, upon encountering a recoverable execution error (e.g., SQL syntax error, missing column, or invalid JSON config), it SHALL automatically invoke an LLM-based repair agent to suggest a correction.

#### Scenario: SQL Syntax Error Recovery
- **WHEN** a `core.sql` node fails due to a syntax error
- **THEN** the engine SHALL capture the error message and the offending SQL
- **AND** it SHALL call the AI repair agent to generate a corrected SQL query
- **AND** it SHALL attempt to re-execute the node with the new query exactly once.

### Requirement: Human-in-the-Loop Escalation
If the automatic repair attempt fails, the system SHALL halt execution for that branch and provide the user with the original error, the failed AI suggestion, and an option to manually edit the configuration.

#### Scenario: Double failure escalation
- **WHEN** the automatic repair query also fails execution
- **THEN** the system SHALL stop retrying
- **AND** it SHALL present a detailed diagnostic report to the user in the console/logs.

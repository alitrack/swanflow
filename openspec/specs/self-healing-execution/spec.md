## ADDED Requirements

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

### Requirement: Human-in-the-Loop Escalation
If the automatic repair attempt fails, the system SHALL halt execution for that branch and provide the user with the original error, the failed AI suggestion, and an option to manually edit the configuration.

#### Scenario: Double failure escalation
- **WHEN** the automatic repair query also fails execution
- **THEN** the system SHALL stop retrying
- **AND** it SHALL present a detailed diagnostic report to the user in the console/logs.

### Requirement: LLM Repair Agent Output
`OpenAiHook::try_fix` SHALL return a `RepairOperator` instead of a bare config value. The fix prompt SHALL include the full graph topology and operator selection rules. Responses in the legacy format (`{"operator":"REBIND","fixed_config":...}` or bare `{"fixed_config":...}`) SHALL remain parseable.

#### Scenario: Operator-bearing fix response
- **WHEN** a node fails with a classified, recoverable error
- **THEN** `OpenAiHook::try_fix` SHALL return a typed `RepairOperator` (one of `REBIND | INSERT_PREREQ | SUBSTITUTE | REWIRE | BYPASS`) with the node id, classified failure type, and any proposed config/edge/node payload
- **AND** the fix prompt sent to the LLM SHALL include the full graph topology and the operator selection rules.

#### Scenario: Legacy response remains parseable
- **WHEN** the LLM responds in the legacy format (`{"operator":"REBIND","fixed_config":{...}}` or a bare `{"fixed_config":{...}}`)
- **THEN** the parser SHALL still decode it into the equivalent `RepairOperator`
- **AND** the engine SHALL re-execute the failed node exactly once with the repaired config.

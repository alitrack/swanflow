## ADDED Requirements

### Requirement: AI-Driven Workflow Branching
The system SHALL provide an `AiRouter` node that inspects the profile of a dataset (column count, null ratio, semantic content) and dynamically determines which of two or more downstream branches should be executed.

#### Scenario: Branch selection based on quality
- **WHEN** the `ProfilerNode` detects a null ratio above 10% in a critical column
- **THEN** the `AiRouter` SHALL redirect the workflow to a cleaning branch
- **AND** it SHALL bypass the primary analytics branch.

### Requirement: Meta-Variable Modification
The `AiRouter` SHALL be able to update global context variables and node configurations on-the-fly, allowing subsequent nodes to be parameterized based on real-time findings.

#### Scenario: Dynamic node parameterization
- **WHEN** the router identifies the input dataset as "Pharma Sector Data"
- **THEN** it SHALL update a global variable `${context.analysis_focus}` to "Clinical Trials".

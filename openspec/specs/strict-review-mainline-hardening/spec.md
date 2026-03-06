# strict-review-mainline-hardening Specification

## Purpose
Define a deterministic strict-review workflow for full-repository audit, severity-based remediation, and evidence-backed closure on mainline.
## Requirements
### Requirement: Deterministic Full-Repository Audit Baseline

The system SHALL provide a deterministic strict-audit baseline that records reproducible outputs for formatting, linting, tests, and static risk scanning across the full repository.

#### Scenario: Baseline audit emits reproducible logs

- **WHEN** a strict-audit run is executed for the repository
- **THEN** the run SHALL execute formatting, linting, test, and static risk checks
- **AND** the run SHALL write each check output to timestamped audit artifacts.

### Requirement: Severity-Based Remediation Policy

The system SHALL classify findings as `P0`, `P1`, or `P2` and SHALL enforce delivery cadence by severity.

#### Scenario: P0 finding handling

- **WHEN** a finding is classified as `P0`
- **THEN** remediation SHALL follow a test-first fix loop
- **AND** the fix SHALL be committed and pushed immediately after verification.

#### Scenario: P1 and P2 finding handling

- **WHEN** findings are classified as `P1` or `P2`
- **THEN** remediation SHALL be grouped into subsystem batches
- **AND** each batch SHALL be committed and pushed only after verification.

### Requirement: OpenSpec Synchronization for Findings and Fixes

The workflow SHALL keep OpenSpec task state synchronized with implemented changes and verification evidence.

#### Scenario: Task synchronization after remediation

- **WHEN** a remediation commit is completed
- **THEN** corresponding OpenSpec tasks SHALL be updated to reflect completed status
- **AND** task state SHALL remain consistent with repository history.

### Requirement: Strict Closure Gate

The change SHALL NOT be closed until a final full verification rerun succeeds and a closure report records fixed findings and residual risks.

#### Scenario: Closure eligibility

- **WHEN** all planned remediation tasks are marked complete
- **THEN** the system SHALL require final full-audit verification to pass
- **AND** a closure report SHALL document final gate results and remaining risks before archive.

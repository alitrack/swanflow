## ADDED Requirements

### Requirement: Fact & Claim Verification Contract
The system SHALL expose a fact/claim verifier node for deterministic claim evidence scoring.

#### Scenario: Claim support scoring
- **WHEN** a claim and evidence snippets are provided
- **THEN** the node SHALL compute a deterministic support score
- **AND** return a verdict based on configured threshold.

#### Scenario: Evidence requirement guardrail
- **WHEN** evidence is missing and `require_evidence=true`
- **THEN** the node SHALL return `insufficient_evidence` verdict.

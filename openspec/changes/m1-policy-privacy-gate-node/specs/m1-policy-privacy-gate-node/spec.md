## ADDED Requirements

### Requirement: Policy & Privacy Gating Contract
The system SHALL expose a policy/privacy gate node for deterministic execution decisions.

#### Scenario: Deny on PII risk
- **WHEN** `block_when_pii=true` and upstream indicates PII is present
- **THEN** the node SHALL return a deny decision with reason.

#### Scenario: Approval marker
- **WHEN** action is listed in approval actions and otherwise allowed
- **THEN** the node SHALL return `requires_approval=true`.

## ADDED Requirements

### Requirement: Embedding Generator Contract
The system SHALL expose an embedding generator node for deterministic text vectorization.

#### Scenario: Deterministic embedding generation
- **WHEN** the same text list is embedded with the same config
- **THEN** output vectors SHALL be deterministic and dimension-consistent.

#### Scenario: Embedding config guardrails
- **WHEN** `embedding_dim` is zero or invalid
- **THEN** node validation SHALL fail with a clear configuration error.

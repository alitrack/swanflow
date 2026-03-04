## ADDED Requirements

### Requirement: PII Masking Contract
The system SHALL expose a reversible PII masking node for outbound-safe content processing.

#### Scenario: Mask detected PII spans
- **WHEN** input text contains supported PII values
- **THEN** the node SHALL return text with deterministic placeholder tokens
- **AND** emit replacement mappings for restoration.

#### Scenario: De-mask restoration
- **WHEN** masked text and replacement mappings are provided
- **THEN** the node SHALL restore original values deterministically.

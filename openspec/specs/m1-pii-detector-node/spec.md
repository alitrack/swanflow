# m1-pii-detector-node Specification

## Purpose
TBD - created by archiving change m1-pii-detector-node. Update Purpose after archive.
## Requirements
### Requirement: PII Detection Contract
The system SHALL expose a PII detector node for deterministic sensitive token detection.

#### Scenario: Detect common PII spans
- **WHEN** input text includes supported PII categories
- **THEN** the node SHALL return matched spans with deterministic confidence scores.

#### Scenario: Category filtering
- **WHEN** categories are configured
- **THEN** the node SHALL only return matches from allowed categories.


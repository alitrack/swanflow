## Context

PII detection should be available as a baseline capability without external model dependencies.

## Design

- Implement `PiiDetectorNode` in `src/nodes/ai/privacy.rs`.
- Use deterministic regex patterns for `email`, `phone`, `ssn`, and `credit_card`.
- Enforce config validation for `min_confidence` and category whitelist.

## Rollout

1. Add failing tests for detection, filtering, validation, and registration.
2. Implement node with deterministic detection helpers.
3. Run targeted regressions and finalize artifacts.

## Context

This change extends retrieval quality while staying contract-first and deterministic.

## Design

- Reuse `retrieval.rs` as M1 retrieval extension module.
- Keep node runtime behavior pass-through initially, with deterministic helper methods for scoring logic.
- Enforce config validation for guardrails.

## Rollout

1. Add tests (red).
2. Implement minimal node contracts (green).
3. Register nodes + run regressions.

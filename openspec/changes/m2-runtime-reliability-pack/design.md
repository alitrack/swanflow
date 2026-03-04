## Context

M2 strategic capabilities depend on deterministic control over retries, state, checkpoints, and audit traces.

## Design

- Build a new `runtime` module under `src/nodes/ai`.
- Implement helper-first deterministic logic for each M2 capability.
- Keep planning behavior pass-through while preserving strict config validation.

## Rollout

1. Add failing tests for all five node capabilities.
2. Implement minimal deterministic behavior.
3. Register nodes and run targeted regressions.

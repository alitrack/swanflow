## Context

Privacy detection/masking is in place, but execution needs a deterministic policy gate.

## Design

- Add `PolicyPrivacyGateNode` with `evaluate_policy(action, has_pii, config)`.
- Support `block_when_pii`, `blocked_actions`, and `approval_actions`.
- Keep node plan behavior pass-through for contract-first rollout.

## Rollout

1. Add failing tests.
2. Implement node and register.
3. Run targeted regressions and finalize artifacts.

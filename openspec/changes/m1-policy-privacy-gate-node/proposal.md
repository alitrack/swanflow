## Why

The engine needs deterministic policy gating before risky outbound actions.

## What Changes

- Add `core.ai.policy_privacy_gate` node contract.
- Add deterministic policy/privacy decision helper.
- Register node and add tests.

## Impact

- `src/nodes/ai/privacy.rs`
- `src/nodes/registry.rs`
- `tests/m1_policy_privacy_gate_tests.rs`

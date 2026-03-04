## Why

The retrieval pipeline needs an explicit claim verification contract before higher-risk downstream actions.

## What Changes

- Add `core.ai.fact_claim_verifier` node contract.
- Register node in core registry.
- Add deterministic claim/evidence verification tests.

## Impact

- `src/nodes/ai/verification.rs`
- `src/nodes/ai/mod.rs`
- `src/nodes/registry.rs`
- `tests/m1_fact_claim_verifier_tests.rs`

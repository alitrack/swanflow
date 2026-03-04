## Why

The engine needs a dedicated, deterministic PII detector contract before adding masking and policy gates.

## What Changes

- Add `core.ai.pii_detector` node contract.
- Register node in core registry.
- Add deterministic PII detection tests.

## Impact

- `src/nodes/ai/privacy.rs`
- `src/nodes/ai/mod.rs`
- `src/nodes/registry.rs`
- `tests/m1_pii_detector_tests.rs`

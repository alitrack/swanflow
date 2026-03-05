## Why

The engine needs reversible masking before external calls or downstream persistence.

## What Changes

- Add `core.ai.pii_masker` node contract.
- Add deterministic mask and de-mask helpers.
- Register node in core registry with tests.

## Impact

- `src/nodes/ai/privacy.rs`
- `src/nodes/registry.rs`
- `tests/m1_pii_masker_tests.rs`

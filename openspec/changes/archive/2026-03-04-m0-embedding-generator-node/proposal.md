## Why

The M0 chain still lacked a first-class embedding generator contract. This change closes that gap and makes vector workflows explicit and testable.

## What Changes

- Add `core.ai.embedding_generator` node contract.
- Register node in default registry.
- Add deterministic embedding tests and validation checks.

## Impact

- `src/nodes/ai/retrieval.rs`
- `src/nodes/registry.rs`
- `tests/m0_embedding_generator_tests.rs`
- `tests/m0_contract_validation_tests.rs`

## Why

M2 needs a reliability foundation before advanced orchestration can be safely expanded.

## What Changes

- Add five M2 runtime reliability node contracts and deterministic helpers.
- Register nodes in core registry.
- Add targeted tests for behavior, validation, and registration.

## Impact

- `src/nodes/ai/runtime.rs`
- `src/nodes/ai/mod.rs`
- `src/nodes/registry.rs`
- `tests/m2_runtime_reliability_tests.rs`

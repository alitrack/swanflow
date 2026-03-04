## 1. Protocol and Metadata Foundation

- [x] 1.1 Extend node config metadata to include `impl_version` and `config_version` with backward-compatible defaults for legacy workflows.
- [x] 1.2 Add migration hook(s) to node behavior contract with default no-op implementation.
- [x] 1.3 Define deterministic error classification types for `validation`, `migration`, `planning`, and `execution`.

## 2. Compiler Preflight Hardening

- [x] 2.1 Implement compiler preflight pipeline: template resolution -> migration -> validation before `propose_plan`.
- [x] 2.2 Ensure preflight failure returns structured node-scoped errors without panic.
- [x] 2.3 Wire preflight error class and metadata into event emission paths.

## 3. Panic-Free Config Access Refactor

- [x] 3.1 Add typed config extractor helpers for required/optional string, number, bool, and array fields.
- [x] 3.2 Refactor high-risk config reads in `src/nodes/io.rs` and `src/nodes/transform.rs` to use typed helpers (remove panic-prone key access in critical paths).
- [x] 3.3 Refactor high-risk config reads in `src/nodes/sql.rs` and `src/nodes/script/*` to use typed helpers.

## 4. Missing-Node Degraded Loading

- [x] 4.1 Implement placeholder node model/behavior that preserves original `type_id` and raw config.
- [x] 4.2 Update workflow load/registry paths to instantiate placeholder nodes when `type_id` is unknown.
- [x] 4.3 Ensure inspect/infer paths report placeholder node diagnostics and run paths fail gracefully with remediation guidance.

## 5. Self-Healing Integration Update

- [x] 5.1 Gate self-healing hooks to run only on recoverable, classified planning/execution errors after successful preflight.
- [x] 5.2 Ensure validation/migration failures bypass AI repair and emit deterministic preflight diagnostics.

## 6. Verification and Regression Tests

- [x] 6.1 Add compiler/engine tests for validation-first behavior and panic-free failure handling.
- [x] 6.2 Add migration tests covering legacy config defaults and multi-step version upgrades.
- [x] 6.3 Add degraded-loading tests for unknown node types (load, inspect, run failure behavior).
- [x] 6.4 Add/adjust self-healing tests validating error-class gating behavior.

## Why

RustFlow currently has high implementation velocity but insufficient runtime stability guarantees: node configs are not uniformly validated in compile/execute paths, many critical nodes still panic on malformed config, and node configuration evolution lacks a formal versioned migration contract. This blocks reliable KNIME-class reuse and upgrade compatibility.

## What Changes

- Introduce a mandatory pre-execution validation gate so every node config is validated before planning/execution.
- Introduce versioned node configuration metadata (`impl_version`, `config_version`) and deterministic migration hooks from older configs to current schema.
- Add missing-node placeholder behavior for unresolvable/removed node types to preserve workflow loadability and diagnostics instead of hard-failing.
- Refactor high-risk engine surfaces (compiler/engine/io-node boundaries) to replace panic-prone paths with structured errors.
- Add focused test suites for validation coverage, migration correctness, and degraded-load behavior.

## Capabilities

### New Capabilities
- `execution-stability-hardening`: Enforce validation-first execution and panic-free error propagation for workflow compilation/execution.
- `node-config-versioning-migration`: Provide versioned node config contract and migration pipeline with backward compatibility.
- `missing-node-degraded-loading`: Preserve load/inspect capability when node factory/type is missing, with explicit placeholder diagnostics.

### Modified Capabilities
- `self-healing-execution`: Expand requirement scope so self-healing hooks operate on validated/migrated configs and deterministic error classes.

## Impact

- Core engine paths: `src/core/compiler.rs`, `src/core/engine.rs`, `src/core/graph.rs`.
- Node protocol and registry: `src/nodes/mod.rs`, `src/nodes/registry.rs`, high-risk node modules (`src/nodes/io.rs`, `src/nodes/transform.rs`, `src/nodes/sql.rs`, `src/nodes/script/*`).
- Event and error semantics: `NodeEvent::Error` coverage and message structure.
- Tests: compiler/engine/node-level regression tests plus migration/degraded-load scenarios.

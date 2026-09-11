## Context

RustFlow has a strong execution core but currently exposes stability gaps that become critical once workflows must survive schema drift and long-lived reuse (the KNIME baseline). The most visible gaps are: missing global invocation of node `validate()`, panic-prone `unwrap()` usage in high-frequency paths, and the absence of a node config version/migration contract. The engine also lacks a degraded-load strategy for unknown node types, which currently turns compatibility issues into hard load failures.

This change is cross-cutting: it touches compiler, engine, node protocol, registry behavior, and test strategy.

## Goals / Non-Goals

**Goals:**
- Enforce a validation-first execution model: every node config is migrated and validated before planning/execution.
- Introduce explicit node config versioning and deterministic migration.
- Eliminate panic-based failure in high-risk planning/runtime surfaces and replace with structured errors.
- Support degraded loading with placeholder nodes when node implementations are unavailable.
- Keep existing workflow authoring format usable via backward-compatible defaults.

**Non-Goals:**
- Rebuild UI/editor semantics for node history (UI is out of scope).
- Introduce a full event-sourced revision database in this change.
- Rewrite all nodes in one pass; this change hardens high-risk paths and establishes enforceable patterns.

## Decisions

### 1. Introduce a versioned node config contract
**Decision:** Extend node instance config handling with `impl_version` (node implementation version) and `config_version` (schema version), with explicit defaults for legacy workflows.

**Rationale:** Version metadata is required to support deterministic migration and safe long-term compatibility.

**Alternatives considered:**
- Infer versions from workflow-level version only: rejected, too coarse and node-agnostic.
- Store migration state outside node config: rejected, increases coupling and breaks portability.

### 2. Add a migration hook to node behavior
**Decision:** Add a migration hook (`migrate_config`) on node behavior with a default no-op implementation. Compiler preflight applies migrations before validation.

**Rationale:** Migration logic belongs to node owners and must be encapsulated per node type.

**Alternatives considered:**
- Central migration switchboard in compiler: rejected, becomes unmaintainable and duplicates node semantics.

### 3. Add compiler preflight (migrate -> validate -> classify)
**Decision:** Before any `propose_plan`, compiler runs a preflight pass per node:
1) resolve templating/context, 2) migrate config, 3) validate config, 4) classify failure as recoverable/non-recoverable.

**Rationale:** This creates one deterministic gate for correctness and allows engine hooks (self-healing) to rely on clean inputs.

**Alternatives considered:**
- Validate lazily inside each node only: rejected, inconsistent behavior and incomplete coverage.

### 4. Replace panic-prone extraction with typed config accessors
**Decision:** Introduce reusable typed config extractor helpers and apply them to high-risk nodes first (`io`, `transform`, `sql`, `script`).

**Rationale:** Eliminates common `unwrap()` panic vectors while keeping code localized.

**Alternatives considered:**
- Global ban with bulk rewrite in one commit: rejected, too risky and hard to verify.

### 5. Add missing-node degraded loading
**Decision:** Unknown node types are loaded as explicit placeholder nodes that preserve original config/type metadata and remain inspectable but non-executable.

**Rationale:** Preserves workflow readability and migration workflows instead of hard-breaking on load.

**Alternatives considered:**
- Hard fail on unknown type: rejected, harms backward compatibility.
- Auto-delete unknown nodes: rejected, destroys user intent/data lineage.

### 6. Strengthen self-healing boundary
**Decision:** Self-healing hooks run only on migrated+validated configs and deterministic recoverable error classes.

**Rationale:** Prevents AI repair from acting on structurally invalid configs and reduces false retries.

**Alternatives considered:**
- Keep current broad retry-on-any-error behavior: rejected, high noise and non-deterministic outcomes.

## Risks / Trade-offs

- [Risk] Migration bugs can transform valid legacy configs incorrectly.  
  → Mitigation: table-driven migration tests per node type and version.

- [Risk] Additional preflight may increase compile latency.  
  → Mitigation: keep migration/validation O(nodes), avoid data access in preflight, benchmark against existing compile tests.

- [Risk] Placeholder nodes could mask real deployment issues if overused.  
  → Mitigation: emit explicit warning/error events with node type and remediation guidance.

- [Risk] Partial panic-removal leaves residual unsafe paths.  
  → Mitigation: enforce new helper API for edited modules and add CI grep/check for fresh `unwrap()` in critical modules.

## Migration Plan

1. Introduce protocol changes (`config_version`, migration hook, error classes) behind backward-compatible defaults.
2. Implement compiler preflight and wire validation/migration before planning.
3. Harden high-risk nodes by replacing critical `unwrap()` config paths with typed extractors.
4. Implement placeholder node loading for unknown node types.
5. Update self-healing behavior to consume classified, migrated/validated errors only.
6. Add/expand test suites:
   - legacy config migration
   - invalid config rejection without panic
   - missing-node degraded load and inspect
   - self-healing gating behavior
7. Rollout: release as minor with compatibility note; rollback by disabling preflight gate via feature flag (temporary safeguard) if required.

## Open Questions

- Should `config_version` live inside `node.config` payload or sibling metadata in `NodeInstance` for cleaner schema evolution?
- Do we need a dedicated `NodeEvent::Warning` variant for placeholder nodes, or reuse structured `Error` with severity tags?
- Which node families must be included in phase-1 hardening to claim “panic-free planning path” as done?

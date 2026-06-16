---
id: 8
title: "Layer Dependency Enforcement — Downward-Only CI Gate"
status: "accepted"
date: "2026-06-16"
phase: "v0.1"
deciders: ["lhy"]
tags: ["architecture", "layering", "ci", "nio"]
affects_crates: ["swanflow"]
affects_layers: ["L0", "L0.5", "L1", "L1.5", "L2"]
supersedes: []
superseded_by: []
related: ["ADR-007"]
requires: []
enables: ["Pure L0 types", "Safe refactoring"]
amends: []
fci: []
inv: []
shadow_zones: []
nika_codes: []
timeline: "2026-06-16 — 4 rules, all clean on current codebase"
---

# ADR-008: Layer Dependency Enforcement

## Context

Nika enforces strict downward-only layer dependencies with 4 CI scripts (`check-layers.sh`, `check-l0-dep-fanout.sh`, `check-adr-081-guards.sh`, `cargo-deny`). Without enforcement, module boundaries erode over time as `core/` acquires reverse-dependencies on `nodes/` or `agent/`, making future crate extraction painful.

After ADR-007 (Kernel trait expansion to 17 traits), we now have a clear L0 → L4 layer architecture. This ADR codifies the rules and adds a CI gate.

## Layer Architecture

| Layer | Directories | Role | Restriction |
|-------|-------------|------|-------------|
| **L0** | `core/error.rs` | Pure error types | Zero `use crate::` |
| **L0.5** | `core/kernel/` | Trait definitions | Only `core::error` or same-layer |
| **L1** | `core/*.rs` (non-kernel) | Core infra + orchestration | No `agent/`, `mcp/`, `adapter/` |
| **L1.5** | `nodes/*` | Node implementations | No `adapter/` |
| **L2** | `agent/`, `mcp/`, `adapter/` | External interfaces | Any (leaf layers) |

```
L0  (core/error) → can import NOTHING from the crate
L0.5 (core/kernel) → can import L0 + same layer
L1  (core/* + nodes/*) → can import L0 + L0.5 + L1 same-layer
L2  (agent/mcp/adapter) → can import everything below
```

## Enforcement

A single CI script `scripts/ci/check-layering.sh` checked into `.github/workflows/layering.yml`:

1. **L0**: `core/error.rs` — zero `use crate::` lines allowed
2. **L0.5**: `core/kernel/*.rs` — only `crate::core::error` or `crate::core::kernel` allowed
3. **L1**: `core/*.rs` — no `crate::agent`, `crate::mcp`, `crate::adapter`
4. **L1.5**: `nodes/*.rs` — no `crate::adapter`

All 4 rules pass on the current codebase (zero violations).

## Consequences

**Positive:**
- Prevents accidental circular dependencies between core and interface layers
- Makes future crate extraction (swanflow-core → swanflow-nodes → swanflow-agent) mechanically safe
- Two orders of magnitude simpler than Nika's 4-script enforcement — appropriate for current project size

**Negative:**
- Still permissive: L1 (`core/*`) can reference L1.5 (`nodes/*`). This is by design — the compiler and planner legitimately need to know about node types. Tightening this would require dynamic node discovery (future work).
- Only checks `use crate::` — does not catch trait objects or indirect references via function signatures (e.g., `fn foo(n: &dyn NodeBehavior)`). Acceptable trade-off for CI simplicity.

## Alternatives Considered

1. **Nika's 4-script approach** — overengineered for current scale (single crate, 57 nodes)
2. **cargo-deny workspace-level whitelist** — too coarse; we need per-module rules
3. **No enforcement** — rejected: module boundaries will erode as team grows

## Evidence

- `bash scripts/ci/check-layering.sh` → exit 0, "ALL CLEAN"
- All 85 tests pass
- `cargo check --lib` zero errors

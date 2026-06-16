---
id: 7
title: "Kernel Trait Expansion — From 4 to 17 Atomic Traits"
status: "accepted"
date: "2026-06-16"
phase: "v0.1"
deciders: ["lhy"]
tags: ["architecture", "kernel", "testing", "nio"]
affects_crates: ["swanflow"]
affects_layers: ["L0 (core/kernel)"]
supersedes: []
superseded_by: []
related: ["ADR-006"]
requires: []
enables: ["Mock-all-the-things testing", "Provider abstraction", "Capability model"]
amends: []
fci: ["zero-async-in-L0 deferred", "kernel-mock-crate deferred"]
inv: []
shadow_zones: []
nika_codes: []
timeline: "2026-06-16 — 17 traits + 27 tests in 4 files"
---

# ADR-007: Kernel Trait Expansion

## Context

We shipped Phase 2 with 4 kernel traits (DbRead, DbWrite, FsRead, HttpFetch) + mock implementations — enough for DataFusion-centric testing. The Nika deep-dive (2026-06-16) revealed that Nika maintains ~50 atomic kernel traits across 4 categories (core/ai/runtime/plugin). This expansion bridges the gap to ~17 traits, covering the three categories most relevant to SwanFlow's dataflow engine.

### Current state before this ADR

| Category | Traits | Mock? |
|----------|--------|-------|
| Core I/O | DbRead, DbWrite, FsRead, HttpFetch | Yes |
| AI inference | — | — |
| Runtime lifecycle | — | — |

## Decision

Expand `src/core/kernel.rs` → `src/core/kernel/` directory with 3 trait groups + 1 mock module:

### Core (8 traits)

| Trait | Methods | Purpose |
|-------|---------|---------|
| `DbRead` | `query`, `list_tables` | Read-only database access |
| `DbWrite` | `execute` | Write-only database access |
| `FsRead` | `read_to_string`, `exists`, `ls` | Read-only filesystem |
| `HttpFetch` | `get`, `post_json` | HTTP client |
| `KeyStore` | `get`, `set`, `delete`, `list_keys` | Key-value / secret storage |
| `Clock` | `now_unix_secs`, `sleep`, `elapsed_secs` | Time abstraction |
| `BlobStore` | `put`, `get`, `delete`, `exists` | Binary blob storage |
| `ShellExec` | `run` → `ShellOutput` | Command execution |
| `ProcessManager` | `list_processes`, `signal` | Process lifecycle |

### AI (3 traits)

| Trait | Methods | Purpose |
|-------|---------|---------|
| `LLMInfer` | `complete(LlmRequest) → LlmResponse` | LLM inference |
| `Embedding` | `embed`, `embed_batch`, `dimension` | Text → vector |
| `Tokenize` | `count_tokens`, `encode`, `decode` | Tokenization |

### Runtime (3 traits)

| Trait | Methods | Purpose |
|-------|---------|---------|
| `CancellationToken` | `is_cancelled`, `wait_cancelled`, `cancel` | Cooperative cancel |
| `Timeout` | `with_timeout`, `remaining_secs` | Deadline enforcement |
| `Progress` | `report`, `last_reported`, `complete` | Progress reporting |

### Mock module

All mock implementations live in a single `mock.rs` following Nika's `nika-kernel-mock` pattern: 1:1 mock for every trait, deterministic, no external dependencies.

## Consequences

**Positive:**
- All 17 traits have in-memory mocks → 27 tests, all deterministic, 0.02s runtime
- AI chain nodes (LLMInfer, Embedding) can be unit-tested without real API keys
- Runtime traits enable cancellation & timeout testing for long-running workflows
- Backward compatible: `pub use core::*` and `pub use mock::*` in `mod.rs` preserves all existing imports
- `json!` dependency resolved: `planner.rs` now directly `use serde_json::json;`

**Negative:**
- 14 new traits to maintain as SwanFlow grows
- `SwanFlowError` doesn't derive `Clone` → some mock patterns (MockShellExec) need manual error rebuilding
- Single `mock.rs` will grow large as more traits are added; future split into `mock/` sub-dir may be needed

**Trade-offs:**
- `KeyStore` vs `SecretStore`: chose flat `KeyStore` (simpler) over Nika's `SecretStore` (encrypted-by-default). Can upgrade later.
- `Clock::sleep` takes `f64` seconds (favoring test ergonomics) not `Duration`. `SystemClock` delegates to `tokio::time::sleep`.
- Not yet separating into a `swanflow-kernel-mock` crate; keep in single crate for now (ADR-006 discipline: split when >1500 LOC).

## Evidence

- 27 kernel tests pass in 0.02s
- `cargo check --lib` zero errors
- Pre-existing mustache template test failures unchanged (2 known)

## Alternatives Considered

1. **Keep 4 traits** — rejected: AI Agent nodes (`nodes/ai/`) need LLM/Embedding traits for testability
2. **One giant trait** — rejected: violates Interface Segregation Principle
3. **Nika's 50 traits** — deferred: SwanFlow doesn't need ScreenCapture/OCR/A11y/BM25 yet

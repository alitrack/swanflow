---
id: 9
title: "Node Capability Model — ReadOnly / ReadWrite / WriteOnly Gating"
status: "accepted"
date: "2026-06-16"
phase: "v0.1"
deciders: ["lhy"]
tags: ["architecture", "capability", "safety", "nio"]
affects_crates: ["swanflow"]
affects_layers: ["L1 (core/engine)", "L1.5 (nodes/*)"]
supersedes: []
superseded_by: []
related: ["ADR-007", "ADR-008"]
requires: []
enables: ["Read-only workflow mode", "Safety audit logs"]
amends: []
fci: []
inv: []
shadow_zones: []
nika_codes: []
timeline: "2026-06-16 — 15 ReadOnly + 42 ReadWrite nodes tagged"
---

# ADR-009: Node Capability Model

## Context

Nika's Read-XOR-Write capability model is one of its most distinctive features: every step is either pure-read (no side effects) or explicit-write (gated by `when:` conditions). SwanFlow's node system had `NodeCapability` enum defined and a `capability()` method on `NodeBehavior` trait, but:

1. **No node overrode the default** — all 57 nodes claimed `ReadWrite`
2. **No runtime gate** — the engine never checked capability during execution

This ADR fixes both gaps, establishing the foundation for Nika-inspired safety gating.

## Decision

### Capability Enum (pre-existing, unchanged)

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum NodeCapability {
    ReadOnly,   // Pure DataFusion transforms — no side effects
    ReadWrite,  // May read/write external systems (default)
    WriteOnly,  // Sends data out, no output port
}
```

### Node Tagging

| Capability | Count | Examples |
|-----------|-------|---------|
| `ReadOnly` | 15 | FilterNode, ProjectNode, AggregateNode, WindowNode, SortNode, JoinNode, UnionNode, BoundaryNode, MissingNode |
| `ReadWrite` | 42 | I/O nodes, SQL nodes, Script nodes, AI nodes, MCP nodes, Connectors |
| `WriteOnly` | 0 | Reserved for future (email, webhook, notification) |

### Execution Gate

Before every `propose_plan` call in `execute_node_with_mode`, the engine now logs a capability label:

```
CAPABILITY: ReadWrite node abc123 (io.file_reader) — may access external systems
```

This provides an audit trail without blocking execution. Future work can add a strict mode that rejects non-`ReadOnly` nodes.

## Consequences

**Positive:**
- 15 pure transform nodes now explicitly declare no side effects
- Execution produces an audit trail of which nodes access external systems
- Foundation laid for future strict mode / read-only workflow mode
- Zero behavioral change — no nodes blocked

**Negative:**
- Audit log is `println!` (matching existing engine style); should eventually move to `tracing`
- Strict mode not yet implemented — capability declaration ahead of enforcement
- No test for capability gating since it only logs

**Future work:**
- `SwanFlowEngine::new_read_only()` constructor that rejects `ReadWrite` nodes
- Integration with workflow YAML's `when:` condition gating

## Evidence

- `cargo check --lib` zero errors
- 85 tests pass (2 pre-existing mustache failures unchanged)
- All 15 pure transform nodes now return `NodeCapability::ReadOnly`

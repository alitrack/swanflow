# ADR-010: AI Orchestration Enhancement

**Status**: Accepted  
**Date**: 2026-06-21  
**Deciders**: SwanFlow maintainers  

## Context

SwanFlow's MCP server (5 tools) already allows external AI agents to orchestrate pipelines — this is the correct architecture. However, the L2 runtime intelligence layer lacks a production-grade implementation:

1. `IntelligenceHook` trait is defined but has **no implementation** — errors cannot self-heal
2. No `ModelRouter` — if an LLM is needed for error recovery, there's no way to select the cheapest capable model
3. No structured `PromptBuilder` — fix prompts would need to be hand-concatenated
4. `execute_sql` MCP tool has **no guard** against write operations — AI agents could execute destructive SQL

**Research** analyzed three comparable systems:

| System | Key Pattern | Suitable for SwanFlow? |
|--------|------------|----------------------|
| OpenFang | `ModelRouter` (375 lines) + `PromptBuilder` (943 lines) — heuristic budget routing + modular prompt assembly | ✅ Yes — both patterns are lightweight and testable |
| NPP Copilot | 2115-line embedded const string prompt — monolithic, unmaintainable | ❌ No — this is the anti-pattern |
| OpenFusion | Multi-model fusion + judge synthesis (Go) — parallel dispatch + blind judge | ❌ No — overkill for a pipeline engine; better as offline eval service |

## Decision

Create an **optional** `swanflow-ai` crate with three components, feature-gated behind `swanflow = { features = ["ai"] }`:

### 1. ModelRouter (inspired by OpenFang `routing.rs`)

Heuristic complexity scoring → budget-aware model selection:

```rust
ErrorKind::Config  → score 1 → Simple  → cheap model (e.g., deepseek-v4-flash)
ErrorKind::Data    → score 2 → Medium  → mid model   (e.g., gemini-2.5-flash)
ErrorKind::Topology→ score 3 → Complex → best model  (e.g., deepseek-v4-pro)
```

Bonuses: +1 if multiple nodes affected, +1 if schema mismatch.

### 2. OpenAiHook

LLM-powered `IntelligenceHook` implementation:
- Classifies error → routes to model → builds prompt → calls LLM → parses fix
- 15-second timeout, single LLM call per error
- Returns `None` gracefully on any failure (timeout, parse error, API key missing)

### 3. PromptBuilder (inspired by OpenFang `prompt_builder.rs`)

Modular context assembly — each field from a separate source:

```rust
PromptContext {
    node_type,          // from NodeSpec
    node_description,   // from NodeSpec
    current_config,     // from runtime
    error_message,      // from error
    available_tools,    // from MCP tool registry
    fix_instructions,   // domain-specific (≤10 lines)
}
```

Max prompt length: 4096 characters. No embedded 200KB const strings.

### 4. SELECT-only Guard in `execute_sql`

Rejects write SQL keywords at MCP protocol level:
```
INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, REPLACE, MERGE
```

Returns `-32602 Invalid params` with descriptive message. This is **structural AI safety** — no prompt can override this guard.

## Consequences

### Positive

- **Zero-cost abstraction**: feature-gated, no code compiled when `ai` feature is off
- **Testable**: each component has dedicated unit tests (12 tests total)
- **Safe by default**: `execute_sql` is now write-protected at protocol level
- **Minimal dependencies**: reuses existing workspace deps (`reqwest`, `serde_json`)
- **Modular prompts**: inspired by OpenFang's structured approach, not NPP's monolithic constant

### Negative

- New crate adds maintenance surface area
- `ModelRouter` heuristics need calibration with real error data
- `OpenAiHook` requires internet connectivity — not suitable for air-gapped deployments

### Neutral

- `IntelligenceHook` trait (in `swanflow-core`) remains unchanged — `swanflow-ai` provides the implementation
- External agents using MCP are unaffected — all changes are additive
- Full workspace tests blocked by pre-existing `psm` build issue (wasmtime dependency)

## Implementation

See `openspec/changes/ai-orchestration-enhancement/` for full specs.

Files created:
- `crates/swanflow-ai/` — new crate (4 modules + tests)
- `crates/swanflow/src/mcp/server.rs` — added `guard_sql()` + call in `execute_sql`
- `crates/swanflow/Cargo.toml` — added `ai` feature + optional dep

Total new code: ~500 lines (excluding tests and comments).

## References

- OpenFang `routing.rs` — heuristic complexity scoring pattern
- OpenFang `prompt_builder.rs` — modular context assembly pattern
- SwanFlow ADR-009 — `WorkflowMode::ReadOnly` capability gating
- NPP architecture review (2026-06-21) — FileOprationTool path traversal as counter-example of prompt-based safety

# Change Proposal: Declarative Middleware Stack (three-point registration)

**Status**: Proposed
**Date**: 2026-08-11
**Change ID**: middleware-stack-compiler
**Source**: Forge borrowing list P1 (research #2026081102)

## Problem

SwanFlow transform nodes (FilterNode, etc.) each implement their own
`propose_plan` — there is no uniform mechanism for "declarative pipeline
preprocessing list → compiled executor". Node configs cannot express
reusable cross-cutting transformations (column selection, row limiting,
sampling) as data; they must be hard-coded per node type. Adding a new
preprocessing capability means writing a new node type and registering it
with the node registry, even when the capability is a thin wrapper around an
existing plan transformation.

Forge solves this with a **middleware stack compiler**: agent nodes declare
`middleware: [{type, config, enabled}]` and the engine compiles the list
into concrete executors via a `MW_BUILDERS` registry. Extension model: add a
middleware = add one builder entry + one config schema entry + one
category-map entry — "then it appears everywhere" (engine/middleware_compiler.py).

## Proposal

Adopt the declarative middleware stack for SwanFlow's plan-level
transformations, keeping it pure Rust (no Python/LangChain):

1. **Declaration**: node config gains optional `middleware: [{type, config, enabled}]`
2. **Registry**: new `crates/swanflow/src/nodes/middleware.rs` with
   `MW_BUILDERS: HashMap<&str, MiddlewareBuilder>` — a builder maps
   `(type, config) → Box<dyn PlanMiddleware>`
3. **Compile**: `compile_middleware_stack(&config) -> Result<Vec<Box<dyn PlanMiddleware>>>`
   — declaration order preserved; `enabled: false` entries skipped
4. **Execution**: `PlanMiddleware::apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan>`
   — applied to each input plan before the node's own `propose_plan` logic

The three-point registration (builder + schema + category map) is
documented in CONTRIBUTING so adding a middleware is a mechanical task.

## Non-Goals

- No LangGraph/LangChain dependency (SwanFlow is DataFusion-based)
- No Python/JS middleware (pure Rust)
- No GUI form generation (P0 covers adapter schema export)
- No modification of uncommitted files (crates/swanflow-core/src/nodes.rs,
  registry.rs M-state, graph-repair-operators change)

## Impact

| Area | Impact |
|---|---|
| New file | `crates/swanflow/src/nodes/middleware.rs` (~150 lines) |
| Modified | `crates/swanflow/src/nodes/config.rs` (middleware parsing helpers), `crates/swanflow/src/nodes/mod.rs` (export), `crates/swanflow/src/lib.rs` (hook into node execution) |
| Tests | `middleware.rs` unit tests: stack order, enabled=false skip, unknown type error |
| Docs | ADR-012, CONTRIBUTING middleware extension section |
| Risk | Low — additive, no existing node behavior changes (middleware only activates when declared) |

## Verification

1. `cargo check --workspace` zero errors
2. New tests: declaration → compiled stack order correct; `enabled: false` skipped; unknown middleware type errors loudly
3. Existing `cargo test -p swanflow-core` baseline (84/84) unchanged
4. No Python introduced

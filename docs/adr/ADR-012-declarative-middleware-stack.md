# ADR-012: Declarative Middleware Stack

**Status**: Accepted
**Date**: 2026-08-11
**Deciders**: SwanFlow maintainers

## Context

SwanFlow transform nodes each implement their own `propose_plan`, but there
was no uniform mechanism for "declarative pipeline preprocessing list →
compiled executor". Cross-cutting transformations (column projection, row
capping, renaming) had to be hard-coded per node type or require new node
types.

**Research** (task #2026081102) analyzed Forge (github.com/nihalashetty/Forge,
MIT, by Nihal A Shetty). Its `engine/middleware_compiler.py` compiles an
agent node's `middleware: [{type, config, enabled}]` declaration into a
concrete middleware list via a `MW_BUILDERS` registry. Its extension model
is the key insight: **add a middleware = add a builder entry + a config
schema entry + a category-map entry — then it appears everywhere**.

SwanFlow is DataFusion-based (logical plan transformations) rather than
LangGraph-based, so the middleware is a **plan transformer**
(`LogicalPlan → LogicalPlan`) rather than an LLM-call wrapper. The pattern
transfers; the framework does not.

## Decision

Add a declarative middleware stack for plan-level transformations:

1. **Declaration**: node config gains optional
   `middleware: [{type, config, enabled}]` (additive, backward compatible —
   nodes without it behave exactly as before).
2. **Registry**: new `crates/swanflow/src/nodes/middleware.rs` holds
   `mw_builders()`, `mw_schemas()`, `mw_categories()` — the three-point
   registration model, all in one file.
3. **Compile**: `compile_middleware_stack(&config)` preserves declaration
   order, skips `enabled: false`, errors loudly on unknown types, and
   validates each instance config via P0's `SchemaValidator` (the schema
   contract triangle applies to middleware too).
4. **Execute**: the engine applies the compiled stack to each input plan of
   a node before calling `propose_plan` (single choke point —
   `crates/swanflow/src/core/engine/mod.rs`, node execution path). No
   per-node cooperation needed.

Initial built-ins: `column_select`, `row_limit`, `column_rename` (all pure
Rust DataFusion plan rewrites).

## Consequences

**Positive:**
- Reusable cross-cutting transformations become data-declarable; adding a
  new node type is no longer required for a thin plan wrapper
- Extension is a mechanical three-entry task in one file (documented in
  CONTRIBUTING)
- Middleware configs are validated by the same schema machinery as node
  configs — consistent error shape (JSON Pointer paths)
- Engine-level choke point means middleware applies uniformly; no node
  forgets to opt in
- Additive: zero behavior change for existing workflows

**Negative:**
- One more concept to document for workflow authors
- Middleware applies only to node **inputs**, not outputs (Forge's
  middleware is also pre-execution only — scope kept minimal)
- `column_rename` requires a full projection rewrite of the plan (cost is
  negligible at plan level; execution re-planning is DataFusion's job)
- datafusion 52 removed the `Sample` node and the `random()` expr_fn, so a
  `sample` middleware was **dropped from the initial set** (would need a
  registered UDF + SessionContext — out of scope); replaced with
  `column_rename`

**Evidence:**
- `cargo test -p swanflow --lib middleware`: 15/15 green (compile-stack
  semantics, built-in behavior, end-to-end order test)
- `cargo check -p swanflow`: zero errors
- No Python/LangChain introduced; no uncommitted files touched

## Alternatives Considered

- **Per-node opt-in inside `propose_plan`**: rejected — easy to forget,
  inconsistent; the engine choke point is the single source of truth.
- **Wrapper node type** (`MiddlewareNode` wrapping an inner node): rejected —
  adds graph complexity and requires workflow authors to insert wrapper
  nodes manually.
- **`sample` via `random()` filter**: rejected — datafusion 52 lacks
  `random()` in `expr_fn`; implementing it requires a registered ScalarUDF
  and a `SessionContext`, breaking the pure-plan design.

# Spec: Declarative Middleware Stack

**Capability**: middleware-stack-compiler
**Change**: middleware-stack-compiler

## Overview

Nodes may declare a middleware stack in their config. The engine compiles
the declaration into ordered plan transformers and applies them to each
input plan before the node's own logic runs. Extension is a three-point
registration in one file.

## Requirements

### R1: Declaration

- Node config MAY contain `"middleware": [...]` — an array of objects:
  - `type` (string, required): middleware type id
  - `config` (object, optional, default `{}`): per-instance config
  - `enabled` (bool, optional, default `true`): when false the entry is
    skipped during compilation
- The field is optional and additive: nodes without it behave exactly as
  before.

### R2: Compilation

- `compile_middleware_stack(config) -> Result<Vec<Box<dyn PlanMiddleware>>>`
- Declaration order is preserved in the output stack.
- Entries with `enabled: false` are skipped silently.
- An unknown `type` produces an error naming the type (no silent pass).
- Malformed middleware `config` (per the middleware's JSON Schema) produces
  an error with a JSON Pointer path into the declaration.

### R3: Execution

- `PlanMiddleware::apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan>`
- The engine applies the compiled stack to each input plan of a node whose
  config declares middleware, before calling `propose_plan`.
- Stack order: first declared = first applied.
- A middleware error propagates as a node execution error (no swallowing).

### R4: Extension (three-point registration)

Adding a middleware requires exactly three entries, all in
`crates/swanflow/src/nodes/middleware.rs`:
1. builder fn + registration in `mw_builders()`
2. config JSON Schema in `mw_schemas()`
3. category string in `mw_categories()`

No other file changes required. Documented in CONTRIBUTING.

### R5: Built-ins

Initial middleware set (all pure Rust, DataFusion plan rewrites):

| type | config schema | behavior |
|---|---|---|
| `column_select` | `{"columns": [string]}` (required) | project only listed columns |
| `row_limit` | `{"limit": int}` (required, >= 0) | cap rows |
| `sample` | `{"fraction": number}` (required, 0 < f <= 1) | sample fraction |

### R6: Validation integration

Middleware configs are validated with `swanflow_core::validation::
SchemaValidator` (P0) against the middleware's schema from `mw_schemas()`.

## Non-Requirements

- No Python/JS middleware
- No LangGraph/LangChain dependency
- No output-side (post-node) middleware in initial scope
- No GUI form generation

# Design: Declarative Middleware Stack

**Change**: middleware-stack-compiler
**Date**: 2026-08-11

## 1. Motivation

SwanFlow nodes transform `LogicalPlan → LogicalPlan`. Several common
transformations are cross-cutting (column projection, row limiting, debug
logging) and today would each require a new node type. Forge's middleware
compiler (engine/middleware_compiler.py) shows the pattern: declare
`middleware: [{type, config, enabled}]` in config, compile via a builder
registry, execute in order. This design ports that pattern to plan-level
middleware.

## 2. Data Model

```rust
/// Declared in node config: `"middleware": [{"type": "column_select", "config": {...}, "enabled": true}]`
pub struct MiddlewareDecl {
    pub type_id: String,
    #[serde(default)]
    pub config: JsonValue,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

/// Compiled executor: transforms an input plan.
pub trait PlanMiddleware: Send + Sync {
    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

/// Builder registry entry: knows the middleware type and how to construct it.
pub type MiddlewareBuilder = fn(&JsonValue) -> Result<Box<dyn PlanMiddleware>>;
```

## 3. Registry (three-point registration)

`crates/swanflow/src/nodes/middleware.rs`:

```rust
pub fn mw_builders() -> HashMap<&'static str, MiddlewareBuilder> {
    let mut m = HashMap::new();
    m.insert("column_select", column_select_builder);
    m.insert("row_limit", row_limit_builder);
    m.insert("sample", sample_builder);
    m
}

/// JSON Schema for each middleware type's config — the contract source.
pub fn mw_schemas() -> HashMap<&'static str, JsonValue> { ... }

/// Category map (documentation/categorization of middleware types).
pub fn mw_categories() -> HashMap<&'static str, &'static str> { ... }
```

**Extension rule** (documented in CONTRIBUTING): to add middleware `X`, add
(1) `X_builder` fn + `m.insert("X", X_builder)`, (2) `"X" → schema` entry in
`mw_schemas()`, (3) `"X" → category` entry in `mw_categories()`. Three
points, all in `middleware.rs` — a mechanical task.

## 4. Compile

```rust
pub fn compile_middleware_stack(config: &JsonValue) -> Result<Vec<Box<dyn PlanMiddleware>>> {
    let decls = config.get("middleware") // Option<&Vec<MiddlewareDecl>>
        .unwrap_or(&vec![]);
    let builders = mw_builders();
    let mut stack = Vec::new();
    for d in decls {
        if !d.enabled { continue; }
        let builder = builders.get(&d.type_id)
            .ok_or_else(|| anyhow!("unknown middleware type '{}'", d.type_id))?;
        stack.push(builder(&d.config)?);
    }
    Ok(stack)
}
```

- Declaration order preserved.
- `enabled: false` skipped silently (like Forge).
- Unknown type → hard error at compile time (no silent pass — same
  discipline as P0's SchemaValidator).

## 5. Execution

Where does middleware apply? Options considered:

- **A. Inside node `propose_plan`** (each node calls `apply_middleware` on
  its inputs first). Pros: per-node opt-in. Cons: every node must remember
  to call it — easy to forget, inconsistent.
- **B. In the engine, before dispatching to a node** (single choke point in
  `SwanFlowEngine::execute` / plan proposal path). Pros: uniform, one place.
  Cons: engine must know which configs carry middleware.
- **C. Wrapper node behavior** (a `MiddlewareNode` that wraps an inner node).

**Decision: B** — engine-level choke point. The engine already reads node
config when proposing plans; middleware applies uniformly to every node that
declares it, no per-node cooperation needed. This matches Forge where
middleware is applied by the compiled graph wrapper, not by agent nodes.

Concretely: in the engine's node-execution path (where it calls
`node.propose_plan(inputs, config)`), before calling, do:

```rust
if let Some(stack) = compiled_cache.get(node_id) {
    inputs = inputs.into_iter().map(|p| stack.apply_all(&p)).collect::<Result<_>>()?;
}
```

Where `apply_all` runs the stack in order and short-circuits on error.

## 6. Config Schema Integration (with P0)

`MiddlewareDecl` configs are validated against `mw_schemas()` using
`swanflow_core::validation::SchemaValidator` — reuses P0's validator. A
malformed middleware config is rejected at compile time with a JSON Pointer
path into the middleware declaration.

## 7. Built-in Middleware (initial set)

| type | purpose | config |
|---|---|---|
| `column_select` | project only listed columns | `{"columns": ["a","b"]}` |
| `row_limit` | cap rows per input | `{"limit": 100}` |
| `sample` | sample fraction of rows | `{"fraction": 0.1}` |

All implemented with DataFusion logical plan rewrites (pure Rust).

## 8. Testing

- `compile_middleware_stack` preserves declaration order
- `enabled: false` entry is skipped
- unknown type errors with the type name
- config validation rejects bad middleware config (schema-driven)
- end-to-end: FilterNode with `row_limit` middleware produces limited plan

## 9. Open Questions

- Q1: Should middleware stack also apply to node **output** plans (post
  node)? Forge middleware is pre-execution only. Initial scope: inputs only.
- Q2: Should middleware be composable with fan-out/collect modes? Initial
  scope: applies per-input-plan regardless of mode (orthogonal).

# Contributing to SwanFlow

## Workflow

- All changes go through OpenSpec: create `openspec/changes/<change-name>/`
  (proposal.md / design.md / specs/<capability>/spec.md / tasks.md), get
  approval, then implement.
- Work on a feature branch (`feat/<change-name>`); merge to `main` only
  after tests pass and the change is reviewed.
- Do not touch uncommitted work belonging to others (check `git status`
  first).

## Adding a middleware (three-point registration)

Middleware is SwanFlow's declarative plan-transformation mechanism (see
`docs/adr/ADR-012-declarative-middleware-stack.md` and
`crates/swanflow/src/nodes/middleware.rs`). To add middleware `X`, make
exactly three entries, all in `crates/swanflow/src/nodes/middleware.rs`:

1. **Builder + registry entry**: implement `XBuilder` (a
   `fn(&JsonValue) -> Result<Box<dyn PlanMiddleware>>`) in the `builtins`
   submodule, then add it to `mw_builders()`:

   ```rust
   m.insert("X", builtins::x_builder);
   ```

2. **Config JSON Schema**: add `x_schema()` and register it in
   `mw_schemas()` — this schema drives validation (via P0's
   `SchemaValidator`) and any future external consumer:

   ```rust
   m.insert("X", builtins::x_schema());
   ```

3. **Category**: add a category string in `mw_categories()` (used for
   documentation / grouping):

   ```rust
   m.insert("X", "shape"); // or "quality", "io", ...
   ```

That's it — no other files change. Then add unit tests for the builder
(valid config, missing config) and for `apply()` behavior.

### Middleware contract

- `PlanMiddleware::apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan>`
  must be a pure plan rewrite (no I/O, no external engine, no Python).
- The stack is applied to each input plan of a node **before** the node's
  own `propose_plan` logic.
- Errors must be descriptive (name the middleware and the failing config
  value); unknown middleware types fail loudly at compile time.

## Testing

- `cargo check --workspace` must be zero-error.
- Run crate tests with the Python bridge available:

  ```sh
  export LD_LIBRARY_PATH=/home/lhy/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib
  CARGO_BUILD_JOBS=4 cargo test -p swanflow --lib <filter>
  ```

- Baseline: `swanflow-core` 84/84 green; main crate 129 green + known
  pre-existing failures (templating mustache, duckdb in-memory table) that
  are unrelated to schema/middleware work.

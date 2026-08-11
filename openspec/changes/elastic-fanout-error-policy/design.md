# Design: Elastic Fan-Out Error Policy

**Change**: elastic-fanout-error-policy
**Date**: 2026-08-11

## 1. Current fan-out flow (engine/mod.rs ~L1222-1294)

```
fan_out node
  ├─ collect children (outgoing edges)
  ├─ tokio::spawn each child → compile_and_execute_node
  ├─ await all handles:
  │    Ok(Ok(output))  → outputs.push(output)
  │    Ok(Err(e))      → outputs.push(format!("Error: {}", e))   ← unconditional
  │    Err(join)       → outputs.push(format!("JoinError: {}", join))
  └─ return Ok(outputs.join("\n---\n"))                           ← cannot fail
```

Problems:
- `error_mode: fail` children never cause the step to fail.
- Failed children are indistinguishable from legitimately-produced
  `"Error: ..."` strings in output.
- No `NodeEvent::Error` is emitted, so observers (UI/CLI) see nothing.

## 2. Target flow

```
fan_out node
  ├─ collect children (unchanged)
  ├─ spawn each child with a wrapper that:
  │     1. runs compile_and_execute_node once, with retry if
  │        child.error_mode == Retry(n) (reuse the single-node retry loop
  │        pattern from execute_node_with_mode)
  │     2. classifies the outcome:
  │          Ok(output)            → Ok(output)
  │          Err(e)                → per child.error_mode:
  │              Fail              → Err(e)          (hard failure)
  │              Skip              → Ok("Error: ...") + emit NodeEvent::Error
  │              Retry(n) exhausted→ per Fail/Skip fallback below
  │     3. retry fallback: after retries exhausted, treat as Fail
  │        (matches single-node path: !retried → should_fail = true)
  ├─ await all handles, then aggregate:
  │     any Err → return Err(aggregated child errors)   [fail policy]
  │     all Ok  → joined outputs (skip policy outputs carry "Error:" strings)
  └─ emit NodeEvent::StateChanged(failed) when step fails
```

## 3. Key decisions

**D1 — Per-child policy, not fan-out-wide.**
Each child keeps its own `error_mode`. Rationale: matches the existing
single-node contract; the fan-out node is an execution-mode attribute, not
a policy owner. A workflow author sets `error_mode: skip` on the children
they want to tolerate.

**D2 — Fail policy is the default (ErrorMode::default() == Fail).**
Today's implicit "always continue" behavior is a foot-gun: a failed
fan-out branch silently poisons the joined output. Forge's semantics also
require an explicit `error_policy: continue` to enable tolerance. Changing
the default to the existing `Fail` default makes fan-out consistent with
single-node execution. Workflows that relied on unconditional tolerance
must set `error_mode: skip` on fan-out children — a one-line, explicit
migration.

**D3 — Skip emits NodeEvent::Error but returns Ok.**
The failure is visible to observers while the superstep continues — the
Forge principle "log loudly, route explicitly".

**D4 — Retry reuse.**
Extract the retry loop from `execute_node_with_mode` (L1053-1079) into a
small helper `run_with_retry(node, ...) -> Result` so both paths share one
implementation. No behavior change for the single-node path.

## 4. Edge cases

- **Empty children** (fan-out with no outgoing edges): unchanged — executes
  the fan-out node itself normally.
- **Join error** (child panicked): treat as a hard error regardless of
  policy (a panic is never "skip").
- **Mixed policies**: children with `skip` keep going when siblings with
  `fail` abort the step; the step aborts on the first `Err` returned by
  aggregation, but all spawned children are still awaited (no orphaned
  tasks; join semantics preserved).

## 5. Testing

- Unit-test the classification logic (pure fn mapping
  `(error_mode, result) -> Outcome`) — no engine needed.
- Integration: construct a small workflow with a fan-out whose children are
  two filter/error nodes, one failing; assert:
  - `fail` child → engine returns Err
  - `skip` child → engine returns Ok with "Error:" in joined output
  - `retry(2)` child → retried (count via event stream)

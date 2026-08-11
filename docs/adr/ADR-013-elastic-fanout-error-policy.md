# ADR-013: Elastic Fan-Out Error Policy

**Status**: Accepted
**Date**: 2026-08-11
**Deciders**: SwanFlow maintainers

## Context

SwanFlow's `fan_out` mode executes child nodes in parallel. The parallel
path was **unconditionally fault-tolerant**: any child failure became a
`"Error: ..."` string in the joined output and the superstep continued
silently. The node-level `error_mode` mechanism (Fail / Skip / Retry) that
governs single-node execution was **not honored** in the fan-out path, so a
workflow author could not express "this fan-out branch must fail loudly"
or "this branch is optional, keep going".

**Research** (task #2026081102) analyzed Forge
(github.com/nihalashetty/Forge, MIT, by Nihal A Shetty). Its workflow
compiler (`engine/compiler.py`) implements **elastic fan-out**: with
`error_policy: continue`, parallel fan-out children are wrapped in fault
tolerance so one failure does not sink the whole superstep. The key
insight is that tolerance is **explicitly selected** — never implicit.
Forge's own debugging principle: log loudly, route explicitly.

SwanFlow already had the policy vocabulary (`ErrorMode`) — what was missing
was honoring it in the parallel path.

## Decision

Make fan-out honor each child's existing `error_mode`:

1. **Per-child policy**: each fan-out child keeps its own `error_mode`; the
   fan-out node itself does not own a policy (it is an execution-mode
   attribute, not a policy owner).
2. **Fail (default)** → any child failure fails the whole fan-out step:
   `Err` with aggregated messages, `NodeEvent::StateChanged(Error(...))`
   on the fan-out node. This matches `ErrorMode::default()` and single-node
   semantics. Workflows that relied on unconditional tolerance must set
   `error_mode: skip` on the children they want to tolerate — a one-line,
   explicit migration.
3. **Skip** → child failure is recorded as `"Error: ..."` in the joined
   output and a `NodeEvent::Error` is emitted for the child (previously no
   event — failure was invisible).
4. **Retry(n)** → the child is retried up to n times before the Fail/Skip
   classification applies (retries exhausted → treated as Fail, matching
   the single-node path).
5. **Join errors** (panicked child task) are always hard failures,
   regardless of policy.

## Consequences

**Positive:**
- Fan-out failures are now loud and policy-driven; a failed branch can no
  longer silently poison the joined output
- No workflow-format change: `error_mode` already exists on `NodeInstance`
- Consistency between the single-node and parallel paths
- Failures observable via the event bus in both Skip and Fail cases

**Negative:**
- Behavior change for existing fan-out workflows: a failing child now
  aborts the step by default (previously tolerated). Explicit
  `error_mode: skip` restores tolerance.
- The retry loop is duplicated between the single-node path and the
  fan-out path (a shared closure-based helper was attempted but rejected:
  Rust's `FnMut` borrow rules prevent a closure capturing `&mut
  step_context` from returning a future that outlives the call — the two
  sites keep identical loop semantics instead).

**Evidence:**
- `cargo test -p swanflow --lib "engine::tests"`: 7/7 green (classification
  matrix ×4 policies, join-error special case, aggregation with event
  emission)
- `cargo check -p swanflow`: zero errors
- Single-node path byte-identical to pre-change behavior (R6)

## Alternatives Considered

- **Shared retry helper via closure**: rejected — Rust borrow checker
  (captured `&mut` cannot escape `FnMut` closure body); kept identical
  inline loops instead.
- **Fan-out-wide policy override**: rejected — per-child policy matches the
  existing contract and gives finer control.
- **Keep unconditional tolerance**: rejected — silent failure is the
  debugging nightmare Forge explicitly warns against.

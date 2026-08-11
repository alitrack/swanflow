# Change Proposal: Elastic Fan-Out Error Policy

**Status**: Proposed
**Date**: 2026-08-11
**Change ID**: elastic-fanout-error-policy
**Source**: Forge borrowing list P2 (research #2026081102)

## Why

SwanFlow's `fan_out` mode executes child nodes in parallel. Today that path
is **unconditionally fault-tolerant**: any child failure becomes a
`"Error: ..."` string in the joined output and the superstep continues
silently. The node-level `error_mode` mechanism (Fail / Skip / Retry) that
already governs single-node execution is **not honored in the fan-out path**.

Forge (research #2026081102, §3.3) shows the wanted semantics: with
`error_policy: continue`, parallel fan-out children are wrapped in fault
tolerance so one failure does not sink the whole superstep — **explicitly
selected**, not implicit. Failure that is invisible is a debugging
nightmare (Forge's own principle: log loudly, route explicitly).

This change makes fan-out honor the existing `error_mode` contract:

- `error_mode: fail` (default) → any child failure fails the fan-out step
  (loud, explicit)
- `error_mode: skip` → child failure is recorded as an error output, other
  branches continue (today's behavior, now opt-in)
- `error_mode: retry(n)` → child is retried up to n times before the above
  policy applies

## Current State (verified)

| Path | Behavior |
|---|---|
| Single node (`execute_node_with_mode`) | `error_mode` honored: Fail → `NodeEvent::Error` + step fails; Skip → continue; Retry(n) → retries |
| Fan-out children (`engine/mod.rs` fan-out block, ~L1222-1294) | `Ok(Err(e)) => outputs.push("Error: ...")` — ignores `error_mode`, always continues |
| Fan-out aggregate failure | Impossible by construction — no `Err` can escape |

## What We Will Do

1. **Honor `error_mode` in the fan-out path**: each child's failure is
   classified by the child's own `error_mode` (children keep their
   individual error modes; the fan-out node does not need its own).
2. **fail policy** → fan-out returns `Err` (aggregating child errors),
   `NodeEvent::Error` emitted per failed child, `NodeEvent::StateChanged`
   marks the fan-out step failed.
3. **skip policy** → today's behavior (error string in output), plus a
   `NodeEvent::Error` so the failure is visible to observers (new).
4. **retry policy** → reuse the single-node retry loop for the child before
   classifying.
5. No workflow-format change: `error_mode` already exists on `NodeInstance`.

## Out of Scope

- New error policies beyond the existing Fail/Skip/Retry
- Fan-out-wide policy override on the fan-out node itself
- Collect-mode failure propagation (collect reads materialized tables;
  missing tables from skipped children are handled downstream as today)

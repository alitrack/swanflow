# Spec: Elastic Fan-Out Error Policy

**Capability**: elastic-fanout-error-policy
**Change**: elastic-fanout-error-policy

## Overview

Fan-out parallel execution honors each child node's existing `error_mode`
(Fail/Skip/Retry) instead of unconditionally tolerating child failures.

## Requirements

### R1 — Fail policy aborts the step
When a fan-out child with `error_mode: fail` fails, the fan-out step
returns an error aggregating the child failures, and emits
`NodeEvent::StateChanged` marking the step failed. **Default** (matches
`ErrorMode::default()`).

### R2 — Skip policy continues with visible errors
When a fan-out child with `error_mode: skip` fails, execution continues:
the joined output contains `"Error: <message>"` for that child, and a
`NodeEvent::Error` is emitted for the child (observability).

### R3 — Retry policy retries before classifying
When a fan-out child with `error_mode: retry(n)` fails, it is retried up
to `n` times before the Fail/Skip classification applies. After retries
are exhausted the child is treated as Fail.

### R4 — Join errors are always hard
A panicked child task (tokio join error) is a hard failure regardless of
policy.

### R5 — All children are awaited
Even when the step will abort (first `Err`), every spawned child is
awaited before the step returns — no orphaned tasks, join semantics
preserved.

### R6 — Single-node path unchanged
The existing `execute_node_with_mode` error handling is refactored into a
shared helper without behavioral change (verified by existing tests).

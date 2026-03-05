## Why

Mainline currently lacks a deterministic strict-audit workflow that combines reproducible evidence collection with severity-based remediation and controlled delivery. We need this now to reduce regression risk while enabling continuous fixes directly on `origin/main`.

## What Changes

- Add a strict full-repository audit flow with reproducible command and log outputs.
- Add a severity policy (`P0/P1/P2`) with explicit remediation and delivery cadence.
- Add OpenSpec-governed tracking to keep findings, fixes, and verification status synchronized.
- Add closure criteria that require end-to-end verification evidence before completion.

## Capabilities

### New Capabilities

- `strict-review-mainline-hardening`: Deterministic strict audit and severity-based remediation workflow with OpenSpec traceability and mainline delivery gates.

### Modified Capabilities

- None.

## Impact

- Affected code areas: `scripts/`, `tests/`, `docs/reviews/`, `openspec/changes/`.
- Delivery process impact: introduces explicit `P0` immediate push and `P1/P2` batch push policy.
- Tooling impact: requires Rust toolchain bootstrap and OpenSpec artifact synchronization during remediation.

## Context

The repository includes Rust runtime modules, integration tests, sidecars, scripts, and OpenSpec artifacts. Prior work improved reliability, but strict review is still ad hoc and does not enforce a single evidence-first loop from detection to fix to delivery. The requested mode requires continuous commits and pushes to `origin/main`, so we need a controlled process that minimizes risk while preserving speed.

## Goals / Non-Goals

**Goals:**
- Establish deterministic full-repo audit gates with reproducible logs.
- Classify findings into `P0/P1/P2` and enforce remediation policy by severity.
- Keep OpenSpec artifacts synchronized with code and verification progress.
- Enforce closure only after full verification and explicit residual-risk reporting.

**Non-Goals:**
- Re-architecting unrelated modules outside identified findings.
- Replacing existing OpenSpec schema or repository-wide workflow conventions.
- Introducing speculative optimizations without evidence from audit results.

## Decisions

### Decision 1: Evidence-first audit gate before remediation

Run strict baseline checks (`fmt`, `clippy`, `test`, static risk scan) before broad code changes.

- Alternative considered: fix-forward on first observed failure.
- Rationale: evidence-first preserves reproducibility, prevents hidden regressions, and supports accurate severity triage.

### Decision 2: Severity-based remediation and delivery

Use strict policy:
- `P0`: immediate test-first fix, immediate commit, immediate push.
- `P1/P2`: subsystem-batched test-first fixes, validated commit, then push.

- Alternative considered: uniform batch delivery for all severities.
- Rationale: `P0` needs shortest time-to-remediation while lower severities benefit from batching efficiency.

### Decision 3: OpenSpec as control plane for audit and fixes

Track lifecycle in one change artifact set (`proposal`, `design`, `spec`, `tasks`) and keep task state aligned with commits.

- Alternative considered: rely only on ad hoc markdown logs.
- Rationale: OpenSpec gives structured traceability and explicit completion gating.

### Decision 4: Closure requires full rerun verification

No closure until final full-audit rerun succeeds and closure report is produced.

- Alternative considered: close after local targeted tests pass.
- Rationale: strict review requires end-to-end confidence, not partial green checks.

## Risks / Trade-offs

- [Risk] Continuous commits on `main` can expose partial states if verification is incomplete.
  - Mitigation: enforce test-first loops and push only after task-level verification.
- [Risk] Full-repo checks may be slow, reducing iteration speed.
  - Mitigation: keep `P0` loops minimal and batch `P1/P2` by subsystem.
- [Risk] Static scans can produce false positives.
  - Mitigation: treat scan output as triage input; require repro/impact evidence before classification.
- [Risk] Dependency/toolchain bootstrap can fail in constrained environments.
  - Mitigation: record blocker evidence and continue static-only audit until environment recovery.

## Migration Plan

1. Create OpenSpec control change and task map.
2. Bootstrap Rust toolchain deterministically.
3. Run initial strict full-repo audit and produce evidence logs.
4. Classify findings and execute severity-based remediation loops.
5. Rerun full verification, produce closure report, and archive change.

Rollback strategy:
- Revert offending commit(s) by SHA when a fix causes regression.
- Reopen affected OpenSpec task(s) and rerun verification before re-push.

## Open Questions

- Should static risk scan scope expand to `examples/` and `tutorials/` code blocks that are executable?
- Should future CI enforce this exact strict-audit script on every merge to `main`?

# Strict Full-Repo Review with Opsx-Guided Development

Date: 2026-03-05
Owner: Codex + User

## 1. Context

The repository is a Rust-based workflow engine with a broad code surface:

- Runtime core and nodes in `src/`
- Large regression/integration test suite in `tests/`
- Python sidecars in `sidecars/`
- Shell and helper scripts in `scripts/`
- OpenSpec artifacts in `openspec/`

The requested mode is strict review plus immediate remediation, with continuous delivery to remote `main`.

## 2. Goals

- Perform a strict full-repo audit (correctness, architecture, test coverage, performance/security risk, OpenSpec alignment).
- Install Rust toolchain automatically and establish reproducible baseline checks.
- Fix issues during review using severity-based workflow.
- Keep development guided by Opsx/OpenSpec artifacts.
- Commit and push continuously to `origin/main`.

## 3. Hard Constraints

- Review scope: full repository (`src/`, `tests/`, `openspec/`, `sidecars/`, `scripts/`, `examples/`, tutorials/assets).
- Rust installation and full baseline execution are authorized.
- Fixing mode: discover-and-fix continuously during audit.
- Git strategy:
  - P0: commit immediately and push immediately.
  - P1/P2: batch by subsystem and push per batch.
- Target branch: remote `origin/main`.

## 4. Approach Options Considered

### Option A (Selected): Spec-first strict gates

1. Install toolchain and run full baseline to collect evidence first.
2. Classify findings into P0/P1/P2.
3. Execute fixes with severity-specific commit cadence.
4. Track all findings and closures in OpenSpec tasks.

Why selected:
- Strongest alignment with Opsx-guided development.
- Maximizes traceability and auditability.
- Reduces risk of ad hoc patching.

### Option B: Patch-first

Fix immediately after first failures, then backfill specs.
Rejected due to weaker OpenSpec traceability.

### Option C: Domain-wave review

Review by domain waves (`src` -> `tests` -> `sidecars` -> `scripts` -> `openspec`).
Useful but less direct for urgent P0 closure.

## 5. Execution Architecture

### Gate-0: Environment baseline

- Install `rustup` + stable toolchain.
- Verify `rustc`, `cargo`, `rustfmt`, `clippy` are available.

### Gate-1: Full-repo baseline audit

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --all-targets --all-features`
- Static scans for high-risk patterns (`unwrap`, `expect`, `panic!`, `unsafe`) and script/sidecar boundary risks.
- OpenSpec status checks for implementation/spec drift.

### Gate-2: Classification and fix loop

- P0: immediate test-first fix -> verify -> single commit -> immediate push.
- P1/P2: grouped test-first fixes -> verify -> batch commit -> push.

### Gate-3: Opsx synchronization

- Maintain one OpenSpec change for this review/fix effort.
- Keep tasks synced with actual commits and verification outcomes.

## 6. Components and Responsibilities

- Audit Orchestrator: runs baseline and static checks in deterministic order.
- Issue Classifier: assigns P0/P1/P2 using explicit risk criteria.
- Fix Executor: performs test-first remediation.
- OpenSpec Tracker: keeps tasks and status aligned with code.
- Git Delivery Controller: enforces commit/push cadence and traceable messages.

## 7. Data Flow

Input:
- Repository contents and OpenSpec artifacts.

Processing:
- Baseline evidence -> issue extraction -> severity classification -> fix -> regression validation -> OpenSpec update.

Output:
- Audit findings with evidence.
- Remediation commits on `main`.
- Updated OpenSpec progress.

## 8. Failure Handling

- Toolchain/dependency failure: log blocker, continue static-only audit, rerun full checks after recovery.
- Flaky test suspicion: isolate minimal repro, do not downgrade severity without evidence.
- Push failure: keep local commits, retry push after auth/network recovery, no history rewriting.

## 9. Testing and Acceptance Criteria

### Baseline Acceptance

- Rust toolchain installation is successful.
- Baseline commands run and produce reproducible outputs.
- Every failed check has command-level evidence.

### Fix Acceptance

- Each P0/P1 has at least one reproduction or regression test.
- Related tests pass after each fix.
- OpenSpec tasks reflect actual implementation state.

### Delivery Acceptance

- Commit history clearly maps issue -> fix -> validation.
- P0 fixes are pushed immediately.
- P1/P2 fixes are pushed in validated batches.
- Final summary includes fixed items, remaining risks, and next actions.

## 10. Opsx Guidance

- Use OpenSpec as the source of truth for review/fix progress.
- Map findings to tasks before claiming closure.
- Do not mark tasks complete without verification evidence.

# Strict Full-Repo Opsx Review Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Execute a strict full-repository audit, fix findings with TDD, and deliver continuous commits/pushes to `origin/main` under OpenSpec governance.

**Architecture:** Use a spec-first workflow: create one OpenSpec change as the control plane, run deterministic audit commands to collect evidence, classify findings as P0/P1/P2, then remediate with test-first loops. P0 issues are single-fix immediate commits/pushes; P1/P2 are subsystem batches with full verification before push.

**Tech Stack:** Rust (rustup stable, cargo, clippy, rustfmt), Bash, OpenSpec CLI (`openspec`), GitHub remote (`origin/main`)

---

### Task 1: Create OpenSpec Control Change

**Files:**
- Create: `openspec/changes/strict-review-mainline-hardening/proposal.md`
- Create: `openspec/changes/strict-review-mainline-hardening/design.md`
- Create: `openspec/changes/strict-review-mainline-hardening/tasks.md`
- Create: `openspec/changes/strict-review-mainline-hardening/specs/strict-review-mainline-hardening/spec.md`

**Step 1: Write the failing test**

```bash
openspec status --change "strict-review-mainline-hardening" --json
```

Expected: non-zero exit and "change not found".

**Step 2: Run test to verify it fails**

Run: `openspec status --change "strict-review-mainline-hardening" --json`
Expected: FAIL (change does not exist).

**Step 3: Write minimal implementation**

```markdown
# proposal.md
## Why
Mainline needs deterministic strict audit and severity-based remediation.

## What Changes
- Add strict full-repo audit workflow
- Add P0/P1/P2 remediation policy
- Add verification and delivery gates
```

```markdown
# tasks.md
- [ ] 1. Bootstrap Rust toolchain baseline
- [ ] 2. Run full audit gates and collect evidence
- [ ] 3. Classify findings and map to P0/P1/P2
- [ ] 4. Fix all P0 with immediate push policy
- [ ] 5. Fix P1/P2 in validated batches
- [ ] 6. Re-run full verification and close change
```

**Step 4: Run test to verify it passes**

Run: `openspec status --change "strict-review-mainline-hardening" --json`
Expected: PASS with visible artifacts and pending tasks.

**Step 5: Commit**

```bash
git add openspec/changes/strict-review-mainline-hardening
git commit -m "spec: add strict-review-mainline-hardening opsx change scaffold"
git push origin main
```

### Task 2: Add Reproducible Rust Toolchain Bootstrap

**Files:**
- Create: `scripts/dev/install_rust_stable.sh`
- Modify: `README.md`

**Step 1: Write the failing test**

```bash
rustc --version
cargo --version
```

Expected: command not found (fresh machine baseline).

**Step 2: Run test to verify it fails**

Run: `rustc --version || true; cargo --version || true`
Expected: FAIL state captured in terminal output.

**Step 3: Write minimal implementation**

```bash
#!/usr/bin/env bash
set -euo pipefail
curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
source "$HOME/.cargo/env"
rustup component add rustfmt clippy
rustc --version
cargo --version
```

**Step 4: Run test to verify it passes**

Run: `bash scripts/dev/install_rust_stable.sh && source "$HOME/.cargo/env" && rustc --version && cargo --version`
Expected: PASS with stable toolchain version output.

**Step 5: Commit**

```bash
git add scripts/dev/install_rust_stable.sh README.md
git commit -m "chore: add reproducible rust stable bootstrap script"
git push origin main
```

### Task 3: Add Deterministic Full-Audit Runner

**Files:**
- Create: `scripts/audit/full_repo_audit.sh`
- Create: `scripts/audit/static_risk_scan.sh`
- Create: `artifacts/audit/.gitkeep`

**Step 1: Write the failing test**

```bash
bash scripts/audit/full_repo_audit.sh
```

Expected: file not found (before creation).

**Step 2: Run test to verify it fails**

Run: `bash scripts/audit/full_repo_audit.sh`
Expected: FAIL due to missing script.

**Step 3: Write minimal implementation**

```bash
#!/usr/bin/env bash
set -euo pipefail
TS="$(date +%Y%m%d-%H%M%S)"
OUT="artifacts/audit/${TS}"
mkdir -p "$OUT"
source "$HOME/.cargo/env"
cargo fmt --check >"$OUT/fmt.log" 2>&1 || true
cargo clippy --all-targets --all-features -- -D warnings >"$OUT/clippy.log" 2>&1 || true
cargo test --all-targets --all-features >"$OUT/test.log" 2>&1 || true
bash scripts/audit/static_risk_scan.sh >"$OUT/risk.log" 2>&1 || true
echo "$OUT"
```

```bash
#!/usr/bin/env bash
set -euo pipefail
rg -n "unwrap\\(|expect\\(|panic!\\(|unsafe\\s*\\{" src tests sidecars scripts || true
```

**Step 4: Run test to verify it passes**

Run: `bash scripts/audit/full_repo_audit.sh`
Expected: PASS and prints audit output directory; log files exist in `artifacts/audit/<timestamp>/`.

**Step 5: Commit**

```bash
git add scripts/audit/full_repo_audit.sh scripts/audit/static_risk_scan.sh artifacts/audit/.gitkeep
git commit -m "chore: add deterministic full-repo audit runners"
git push origin main
```

### Task 4: Produce Initial Strict Audit Report

**Files:**
- Create: `docs/reviews/2026-03-05-initial-strict-audit.md`
- Modify: `openspec/changes/strict-review-mainline-hardening/tasks.md`

**Step 1: Write the failing test**

```bash
rg -n "## P0|## P1|## P2|## Evidence" docs/reviews/2026-03-05-initial-strict-audit.md
```

Expected: no matches (file missing).

**Step 2: Run test to verify it fails**

Run: `rg -n "## P0|## P1|## P2|## Evidence" docs/reviews/2026-03-05-initial-strict-audit.md || true`
Expected: FAIL/empty because report is absent.

**Step 3: Write minimal implementation**

```markdown
# Initial Strict Audit (2026-03-05)

## Evidence
- artifacts/audit/<timestamp>/fmt.log
- artifacts/audit/<timestamp>/clippy.log
- artifacts/audit/<timestamp>/test.log
- artifacts/audit/<timestamp>/risk.log

## P0
- [ ] FINDING-ID: summary, file:line, impact, repro

## P1
- [ ] FINDING-ID: summary, file:line, impact, repro

## P2
- [ ] FINDING-ID: summary, file:line, impact, repro
```

**Step 4: Run test to verify it passes**

Run: `rg -n "## P0|## P1|## P2|## Evidence" docs/reviews/2026-03-05-initial-strict-audit.md`
Expected: PASS with four section matches.

**Step 5: Commit**

```bash
git add docs/reviews/2026-03-05-initial-strict-audit.md openspec/changes/strict-review-mainline-hardening/tasks.md
git commit -m "docs: add initial strict audit report and opsx task mapping"
git push origin main
```

### Task 5: P0 Immediate Fix Loop (Repeat Per Finding)

**Files:**
- Create: `tests/review_p0_regressions.rs`
- Modify: `src/...` (exact failing component from finding)
- Modify: `openspec/changes/strict-review-mainline-hardening/tasks.md`
- Modify: `docs/reviews/2026-03-05-initial-strict-audit.md`

**Step 1: Write the failing test**

```rust
#[test]
fn p0_finding_<id>_repro() {
    // minimal repro for the exact P0 bug
    // assert current behavior is wrong/panics
    panic!("replace with concrete repro assertion");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test review_p0_regressions p0_finding_<id>_repro -- --nocapture`
Expected: FAIL reproducing the P0 symptom.

**Step 3: Write minimal implementation**

```rust
// Apply smallest fix in exact source module from stacktrace.
// Avoid broad refactor; include guard/validation/error-class mapping as needed.
```

Use `@systematic-debugging` and `@test-driven-development` for each P0 finding.

**Step 4: Run test to verify it passes**

Run: `cargo test --test review_p0_regressions p0_finding_<id>_repro -- --nocapture`
Expected: PASS.

Then run impacted suite, for example:
`cargo test --test <related_suite> -- --nocapture`

**Step 5: Commit**

```bash
git add tests/review_p0_regressions.rs src/<exact_path> openspec/changes/strict-review-mainline-hardening/tasks.md docs/reviews/2026-03-05-initial-strict-audit.md
git commit -m "fix(p0): resolve <finding-id> <short-scope>"
git push origin main
```

### Task 6: P1/P2 Batch Fix Loop (By Subsystem)

**Files:**
- Create: `tests/review_p1_p2_regressions.rs`
- Modify: `src/...`, `sidecars/...`, `scripts/...` (exact subsystem files)
- Modify: `openspec/changes/strict-review-mainline-hardening/tasks.md`
- Modify: `docs/reviews/2026-03-05-initial-strict-audit.md`

**Step 1: Write the failing test**

```rust
#[test]
fn p1_finding_<id>_repro() {
    // subsystem-level repro
    assert!(false, "replace with concrete failing assertion");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test review_p1_p2_regressions p1_finding_<id>_repro -- --nocapture`
Expected: FAIL.

**Step 3: Write minimal implementation**

```rust
// Minimal fix for one subsystem cluster (io/core/ai/sidecar/script).
// Preserve public behavior unless finding explicitly requires behavior change.
```

Use `@test-driven-development` and `@verification-before-completion` before each batch push.

**Step 4: Run test to verify it passes**

Run:
- `cargo test --test review_p1_p2_regressions -- --nocapture`
- `cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS.

**Step 5: Commit**

```bash
git add tests/review_p1_p2_regressions.rs src/<subsystem_files> sidecars/<files> scripts/<files> openspec/changes/strict-review-mainline-hardening/tasks.md docs/reviews/2026-03-05-initial-strict-audit.md
git commit -m "fix(p1,p2): harden <subsystem> findings batch"
git push origin main
```

### Task 7: OpenSpec Progress Sync and Audit Delta Check

**Files:**
- Modify: `openspec/changes/strict-review-mainline-hardening/tasks.md`
- Modify: `openspec/changes/strict-review-mainline-hardening/design.md`
- Modify: `openspec/changes/strict-review-mainline-hardening/specs/strict-review-mainline-hardening/spec.md`

**Step 1: Write the failing test**

```bash
openspec instructions apply --change "strict-review-mainline-hardening" --json
```

Expected: shows pending items or artifact mismatch.

**Step 2: Run test to verify it fails**

Run: `openspec status --change "strict-review-mainline-hardening" --json`
Expected: pending tasks still present before sync.

**Step 3: Write minimal implementation**

```markdown
- Update tasks checkboxes for completed fixes.
- Record final requirement-level deltas in spec.md.
- Keep design.md aligned with actual implementation decisions.
```

**Step 4: Run test to verify it passes**

Run: `openspec status --change "strict-review-mainline-hardening" --json`
Expected: all implemented tasks marked complete; no unexpected missing artifacts.

**Step 5: Commit**

```bash
git add openspec/changes/strict-review-mainline-hardening
git commit -m "docs(spec): sync strict review opsx artifacts with implemented fixes"
git push origin main
```

### Task 8: Final Verification Gate and Closure Report

**Files:**
- Create: `docs/reviews/2026-03-05-strict-audit-closure.md`
- Modify: `openspec/changes/strict-review-mainline-hardening/tasks.md`

**Step 1: Write the failing test**

```bash
bash scripts/audit/full_repo_audit.sh
```

Expected: any non-empty failure logs block closure.

**Step 2: Run test to verify it fails (if failures remain)**

Run: `bash scripts/audit/full_repo_audit.sh`
Expected: FAIL until all mandatory gates are green.

**Step 3: Write minimal implementation**

```markdown
# Strict Audit Closure

## Final Gate Results
- fmt: PASS/FAIL
- clippy: PASS/FAIL
- tests: PASS/FAIL
- static risk scan: PASS/FAIL

## Fixed Findings
- P0: ...
- P1: ...
- P2: ...

## Residual Risks
- ...
```

**Step 4: Run test to verify it passes**

Run:
- `bash scripts/audit/full_repo_audit.sh`
- `cargo test --all-targets --all-features`
Expected: PASS for closure criteria.

**Step 5: Commit**

```bash
git add docs/reviews/2026-03-05-strict-audit-closure.md openspec/changes/strict-review-mainline-hardening/tasks.md
git commit -m "docs: add strict audit closure report"
git push origin main
```

### Task 9: Archive Completed OpenSpec Change

**Files:**
- Move: `openspec/changes/strict-review-mainline-hardening` -> `openspec/changes/archive/YYYY-MM-DD-strict-review-mainline-hardening`

**Step 1: Write the failing test**

```bash
openspec archive strict-review-mainline-hardening -y
```

Expected: blocked if tasks/spec sync are incomplete.

**Step 2: Run test to verify it fails if not ready**

Run: `openspec archive strict-review-mainline-hardening -y`
Expected: FAIL until all requirements are complete.

**Step 3: Write minimal implementation**

```bash
# Ensure status is complete, then archive:
openspec status --change "strict-review-mainline-hardening" --json
openspec archive strict-review-mainline-hardening -y
```

Use `@openspec-archive-change` for this task.

**Step 4: Run test to verify it passes**

Run: `openspec list --json`
Expected: change no longer in active list and present under archive path.

**Step 5: Commit**

```bash
git add openspec/changes/archive
git commit -m "chore: archive strict-review-mainline-hardening opsx change"
git push origin main
```

## Execution Rules

- Use `@verification-before-completion` before claiming any task complete.
- Never skip the red-green-refactor loop for P0/P1 fixes.
- Keep commits small and reversible.
- Follow delivery policy:
  - P0: immediate commit + immediate push.
  - P1/P2: validated batch commit + push.

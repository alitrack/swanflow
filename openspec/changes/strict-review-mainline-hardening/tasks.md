## 1. OpenSpec Control Plane

- [x] 1.1 Create and validate strict-review change artifacts (proposal, design, specs, tasks).
- [x] 1.2 Map strict-audit execution and remediation policy to explicit task groups.

## 2. Toolchain and Baseline Audit

- [x] 2.1 Add deterministic Rust stable bootstrap script with required components.
- [x] 2.2 Add full-repo audit runner that captures `fmt`, `clippy`, `test`, and static scan logs.
- [x] 2.3 Execute baseline audit and persist timestamped evidence artifacts.

## 3. Findings Classification and Tracking

- [x] 3.1 Produce initial strict-audit report with `P0/P1/P2` sections and evidence links.
- [x] 3.2 Classify each finding with file-level scope, impact, and reproduction notes.
- [x] 3.3 Synchronize finding status between report and OpenSpec tasks.

## 4. P0 Immediate Remediation Loop

- [ ] 4.1 For each P0 finding, add a failing repro/regression test first.
- [ ] 4.2 Implement minimal fix and verify targeted tests pass.
- [ ] 4.3 Commit and push each verified P0 fix immediately to `origin/main`.

## 5. P1/P2 Batched Remediation Loop

- [ ] 5.1 Group P1/P2 findings by subsystem and add regression coverage.
- [ ] 5.2 Implement minimal subsystem fixes and run verification gates.
- [ ] 5.3 Commit and push each verified subsystem batch to `origin/main`.

## 6. Final Verification and Closure

- [ ] 6.1 Rerun full strict-audit baseline and ensure all required gates pass.
- [ ] 6.2 Publish strict-audit closure report with fixed items and residual risks.
- [ ] 6.3 Complete OpenSpec status checks and archive the finished change.

# AGENTS.md — SwanFlow Engineering Constitution

This file is the single source of truth for AI coding agents (and humans)
working in this repository. Keep it in sync with reality: if code changes
invalidate a rule here, update this file in the same change.

## What SwanFlow Is

A Rust-native data science DAG engine: Apache DataFusion 52 + Arrow 57 core,
declarative YAML workflows, zero-copy Python bridge (PyO3 + pyo3-arrow),
WASM UDF sandbox, KNIME (.knwf) import/execution, DBX-compatible connections,
and an AI analyst agent. Five crates: `swanflow-core`, `swanflow`,
`swanflow-ai`, `swanflow-knime`, `swanflow-plugin-sdk`.

## Non-Negotiable Architecture Rules

1. **Layer enforcement is CI-gated, downward-only** (ADR-008,
   `.github/workflows/layering.yml`). Never add an upward import. If a layer
   needs something from above, the design is wrong — propose an ADR instead.
2. **DuckDB is a mandatory dependency** (ADR-004). Do not make it optional,
   do not introduce a second embedded SQL engine.
3. **Nodes are read-XOR-write** (ADR-001). A node either consumes or produces
   a DataFrame-like artifact, never both.
4. **The binary lives in `crates/swanflow/`** — the historical root `src/`
   is a legacy copy. Never edit root `src/` or add to it; audits and changes
   go to `crates/swanflow/src/`.
5. **Module-split discipline** (ADR-005/006): split before files exceed
   reviewable size; one responsibility per module; no cross-module reach-ins.
6. **Adding a middleware = exactly three entries** in
   `crates/swanflow/src/nodes/middleware.rs` (builder + registry + doc row).
   See CONTRIBUTING.md for the walkthrough.

## Commands

```bash
make check    # cargo check --workspace (fast; needs DUCKDB_DOWNLOAD_LIB=1)
make test     # cargo test --workspace
make release  # optimized single binary -> target/release/swanflow
```

CI runs `cargo fmt --check` + `clippy -D warnings` + the layering gate.
Fix warnings at the source; never `allow()` your way past the gate.

## Change Workflow (OpenSpec)

All non-trivial changes go through OpenSpec
(`openspec/changes/<name>/` with proposal.md / design.md / tasks.md /
spec deltas), reviewed, then archived into `openspec/specs/`. Proposals
must state Non-goals; "Why" claims need file:line evidence. Architecture
decisions get an ADR in `docs/adr/` first. Do not start implementation
before the proposal is accepted.

## Dependency Policy

Workspace-managed versions in root `Cargo.toml`; crates reference with
`workspace = true`. Before hand-writing any algorithm, check crates.io for
a mature crate first (hand-rolled code is the fallback, not the default).
Pure-Rust preferred; no new Python/system-tool runtime dependencies in the
engine path.

## Testing Discipline

- Regression tests before cleanup edits when behavior is unprotected.
- Bug fixes require a test that fails on the old code.
- Performance claims require release-mode timing A/B — never trust
  debug-mode numbers. If measurement rejects the optimization, record the
  rejection (negative results are results).
- Run `make test` before every commit.

## Git Discipline

Feature branches (`feat/<change-name>`, `fix/<...>`) → PR to `main`. Never
commit directly to `main`. Never `git add -A` — add files explicitly; other
agents/sessions may have uncommitted work in the same tree. If the working
tree is dirty with changes that are not yours, do not touch or report them;
use `git worktree` for isolated work.

## Documentation

- Before open-sourcing, README should follow the house convention:
  English-default `README.md` + `README_cn.md` (current README is
  Chinese-dominant; convert when the repo goes public).
- Architecture diagrams: Mermaid.
- User-visible changes need a CHANGELOG.md entry in the same PR.
- Generated docs (node registry inventory, examples) must be regenerated in
  the same change that alters their source of truth.

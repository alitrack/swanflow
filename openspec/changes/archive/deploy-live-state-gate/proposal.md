# deploy-live-state-gate — SwapGate call-site wiring

## Why

Follow-up to `add-verified-swap` (fast-anything research #2026112311): the
three-piece pattern needs a real call site. The deploy flow had two
evidence-backed gaps:

1. **Dangling rollback pointer (defect).** `deploy()` set
   `rollback: Some(manifest_file_name(dag_id))` (deployer.rs:117) but never
   wrote the `history-` file it points to — only `rollback()` wrote one. A
   real deploy→deploy→rollback sequence failed with "previous manifest not
   found". The E2E `second_deploy_chains_rollback_pointer` literally
   hand-wrote the history file ("Simulate the previous manifest snapshot"),
   papering over the defect.
2. **No mechanical gate before going live over existing state.** `deploy()`
   shipped over a live deployment without checking that the local
   `flights/` copies of the CURRENT manifest still matched their recorded
   digests — exactly the shape fast-anything's #26/#67 fixed ("build and
   enable require the verified Rust sources").

## What Changes

- `deploy()` now: reads the previous manifest and runs
  `verify_local_sources` BEFORE any MCP connect (zero-call refusal);
  archives the outgoing manifest byte-exact to `history-<name>` so the
  rollback pointer resolves; seals `programs_hash` (FNV-1a over the
  (flight_name, source) set via `core::verified_swap::source_hash`) into
  the manifest.
- `rollback()` reuses the same `verify_local_sources` gate upfront (all
  sources verified before the first `update_flight` — no partial rollback
  on drift).
- New `deployer::check(dag_id, flights_dir, manifest_dir)`: read-only
  integrity probe (no token, no network) — rebuilds the seal from local
  copies and compares via `SwapGate`. Missing seal (`programs_hash: None`,
  pre-sealing manifests) is an explicit error, never a silent pass
  (M1.f lesson: 字段缺失 ≠ 当前默认).
- CLI: `swanflow deploy --check <workflow.yaml>` (conflicts with
  `--rollback`).
- `DeployManifest.programs_hash: Option<String>` with `#[serde(default)]`
  — old manifests still load.
- E2E: the hand-written history simulation is REPLACED by asserting the
  automatic archive; new tests: drift-refusal with zero MCP calls
  (`create_count` stays 1), check() refusal matrix (no manifest / drift
  names both hashes / unsealed manifest).

## Non-goals

- No change to the depth-1 history slot design (single `history-<name>`
  per dag; ping-pong via git-checkout + rollback stays the model).
- No approval-card integration yet (M2) — the seal today is written by
  deploy itself; an approval act can seal later without schema change.
- No per-flight hash algorithm change (sha256 stays for records; the FNV
  seal is additive over the whole set).

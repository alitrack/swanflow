# Changelog

All notable changes to SwanFlow are documented in this file.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) /
SemVer. One entry per change, committed in the same PR as the change.

## [Unreleased]

### Added
- `AGENTS.md` — engineering constitution for AI agents and human
  contributors (architecture rules distilled from ADR-001/002/004/005/006/008,
  commands, OpenSpec workflow, testing and git discipline).
- `SECURITY.md` — private vulnerability reporting channel and support
  policy.
- This changelog. History below is backfilled from git history; entries
  before 2026-10 were reconstructed and carry no PR numbers where the
  change predates the PR workflow.
- `core.io.duckdb_write` gains two Starlake-style write strategies
  (#2026111603 spike), both single-transaction and idempotent by
  construction:
  - `mode: merge` (+ `primary_keys`) — upsert by key: matching target rows
    are replaced, other staging rows inserted.
  - `mode: scd2` (+ `primary_keys`, optional `tracked_columns`,
    `scd2_columns = valid_from,valid_to,is_current`) — type-2 slowly
    changing dimension: a tracked attribute change closes the current row
    (`valid_to` = run timestamp, `is_current = false`) and inserts the new
    version; unchanged rows are left untouched. `tracked_columns` defaults
    to **every non-key column**.
  - Both modes map staging columns **by name** (`INSERT ... BY NAME`, same
    as `append`), so an upstream column reorder no longer writes values
    into the wrong columns.
  - Both modes **reject a batch that carries two rows for the same key**
    with an explicit error: the merge result would otherwise depend on
    batch layout (silent last-wins / duplicated rows).
  - `scd2` fails with a diagnostic (not a raw binder error) when the
    target table exists but lacks the control columns.

### Fixed
- `cargo check` under `RUSTFLAGS=-D warnings` (the CI gate) failed on seven
  unused imports in `swanflow-core` since 2026-10-14, turning every `main`
  push red: `kernel/ai.rs`, `kernel/core.rs`, `nodes.rs`, `registry.rs`.
- Removed the dangling `zeroclaw` gitlink (mode 160000 with no
  `.gitmodules` entry) that made every checkout print
  `fatal: No url found for submodule path 'zeroclaw'`.
- `DuckDBWriteSinkExec` now coalesces multi-partition inputs before
  writing (Bug-7 family) (#2).

### Changed
- ADR-014 Stage 4: webhook push triggers for CDC pipelines (#1).

## [0.1.0] — unreleased baseline

Initial workspace layout: `swanflow-core` (engine), `swanflow` (binary),
`swanflow-ai` (AnalystAgent + IntelligenceHook), `swanflow-knime`
(.knwf parser/mapper/executor), `swanflow-plugin-sdk`.

Core capabilities as of the baseline:
- Declarative YAML DAG workflows; late-binding SQL recompiler
  (parse deferred to physical-plan time; `SELECT * FROM node_uuid`
  node references without explicit edges).
- Apache DataFusion 52 + Arrow 57 execution; DuckDB 1.4 scanners;
  SQLx connectors (Postgres/MySQL/MSSQL); rust_xlsxwriter reporting.
- Zero-copy Python bridge via Arrow C Data Interface (PyO3).
- WASM UDF sandbox; MCP server + client dual channel.
- KNIME workflow import (.knwf/.knar) and execution.
- DBX (Beekeeper Studio) compatible connection profiles.
- Layer-enforcement CI gate (ADR-008); OpenSpec change governance
  (44 capability specs); 14 accepted ADRs.

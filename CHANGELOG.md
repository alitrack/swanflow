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

### Fixed
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

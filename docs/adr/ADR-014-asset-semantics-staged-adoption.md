# ADR-014: Asset Semantics — Staged Adoption

**Status**: Proposed
**Date**: 2026-10-05
**Deciders**: SwanFlow maintainers

## Context

SwanFlow's canvas is **operator-centric**: nodes are transforms, edges are
port-to-port dataflows. This serves interactive analysis well, but three
capabilities are increasingly requested and do not fit the operator model:

1. **Incremental processing** — re-run only what changed (`@incremental_by`)
2. **Partitioned backfill** — re-process historical intervals with calendar
   visibility (✅🟡❌⚪)
3. **Declarative freshness** — "keep this table < 5min stale" without
   hand-wiring schedules

duckOrch (github.com/nkwork9999/duck-orch) demonstrates all three as a
DuckDB extension using a Dagster-style asset model (Phases 13–17). We have
already adopted its cheap ideas:

| Borrowed | Commit | Status |
|----------|--------|--------|
| Table-level SQL lineage (sqlparser-rs, planning-time) | `66982d7` | ✅ shipped |
| Column-level lineage | `4ad0bed` | ✅ shipped |
| `import duckorch` converter (task folders → workflow) | `66fd597` | ✅ shipped |
| Asset semantics (partitions/backfill/freshness) | — | **this ADR** |

The remaining question is *how much* of the asset model SwanFlow needs,
and in what order.

## Research summary

| System | Asset model | Lesson for SwanFlow |
|--------|------------|---------------------|
| Dagster | Software-Defined Assets; partitions + backfill + AutomationCondition | Richest UX; heavy conceptual surface (asset keys, materialization history, sensor loop) |
| duckOrch | `@asset` promoted tasks; `__orch__` metadata tables; interval tracking (SQLMesh-style) | Fits DuckDB-native runtime; interval set-difference backfill is elegant and cheap |
| SQLMesh | Interval-based processing; `${interval_start}` substitution | Gap computation as set difference avoids wall-clock drift |
| dbt | `--full-refresh` + tests | Downstream-skip semantics worth copying |

Key duckOrch mechanisms we would need:

- **Interval tracking**: `__orch__.asset_intervals` stores *which* intervals
  are done. Gaps = set difference; contiguous gaps run as one batch.
  `${interval_start}/${interval_end}` substituted into SQL.
- **Partitions**: `$partition_key` bound via DuckDB `PREPARE`; task SQL
  runs once per partition. Calendar view for humans.
- **Automation DSL**: `eager | on_cron(...) | on_interval(...) |
  freshness_violated` with `&/|/!` operators; stateless evaluator polled
  by a sensor thread, throttled by `target_lag`.

## Decision

Adopt asset semantics **in three stages**, each independently shippable
and each gated on real usage of the previous stage. SwanFlow remains
operator-centric; assets are a *projection* on top of nodes, not a
replacement.

### Stage 1 — Materialization ledger (ship first, small)

Nodes already carry `materialize: bool`. Formalize:

- New DuckDB-backed ledger (`swanflow_cache/assets.duckdb`, same opt-in
  directory governance as extfn catalog):
  `assets(name, node_id, upstream_digest, last_run_at, interval_start, interval_end, status)`
- `upstream_digest` = hash of (node config + upstream node digests +
  input table fingerprints). Digest match → skip. This is **incremental
  processing with none of the interval machinery**.
- CLI: `swanflow assets list [--stale]`, `swanflow assets rebuild <name>`
- Surfaced on canvas as a badge (materialized/stale) — diagnostics
  channel, no new UI concepts.

**Rationale**: 80% of the "why did my table not update" value with 10% of
the machinery. No scheduler, no sensor thread, no partition math.

### Stage 2 — Intervals + backfill

- Add `partitions` to NodeInstance config (daily/static/multi, duckOrch
  syntax).
- Interval tracking in the ledger; `${interval_start}/${interval_end}`
  substitution in SQL nodes (same substitution engine as templating).
- `swanflow assets backfill <name> --from D --to D | --missing` —
  contiguous-gap batching, calendar ASCII output.
- Downstream skip on failure (dbt semantics): failed partition marks
  interval incomplete; downstream intervals depending on it are skipped,
  not silently stale.

### Stage 3 — Freshness + automation conditions

- `automation` config on nodes (the DSL string, evaluated statelessly).
- Sensor loop: a daemon-tick within `swanflow daemon` (already exists),
  NOT a new thread. `target_lag` throttle.
- Freshness surfaces: `assets list --stale` gains `max_lag` violations;
  OpenLineage-style event emission rides the existing MCP event channel.

### Non-goals

- No Snowflake `CREATE DYNAMIC ASSET` syntax.
- No new MCP tool surface beyond `assets list/backfill` (canvas covers
  interactive use).
- No column-level freshness (Stage 2+ if ever).

## Consequences

**Positive**

- Stage 1 is a ledger + digest + two CLI verbs — days, not weeks.
- Every stage reuses existing infra: DuckDB ledger (extfn pattern),
  diagnostics channel, daemon tick, import converter already emits
  `skipped: @partitions_by` warnings that Stage 2 will convert to real
  config.
- duckOrch interop strengthens: `import duckorch` output becomes fully
  functional instead of partially-skipped.

**Negative / risks**

- Ledger is new persistent state → needs the same opt-in governance
  fight as the extfn catalog (won at `623804a`); reuse the pattern.
- Interval substitution in SQL changes query text → interacts with
  lineage extraction (must re-run per interval; digest must include
  interval bounds — addressed in Stage 2 spec).
- Asset vocabulary adds conceptual surface to a canvas that prides
  itself on being operator-simple. Mitigation: assets never appear in
  the base canvas model; they are derived metadata (badges, reports).

## Implementation sketch (Stage 1)

```rust
// crates/swanflow-core/src/assets.rs
pub struct AssetLedger {
    conn: duckdb::Connection, // swanflow_cache/assets.duckdb
}

impl AssetLedger {
    pub fn upstream_digest(&self, node_id: Uuid) -> Option<[u8; 32]>;
    pub fn record_run(&self, node_id: Uuid, digest: [u8; 32], interval: Option<(DateTime, DateTime)>);
    pub fn stale(&self) -> Vec<AssetStatus>; // digest mismatch or missing
}
```

Engine hook: after a node executes, compute digest, compare, record.
`--force` flag bypasses (dbt `--full-refresh` semantics).

## Verification plan

- Stage 1: unit tests for digest computation (config change → stale;
  upstream change → downstream stale); E2E: run twice → second run skips
  materialized nodes; `assets list` output.
- Stage 2: backfill gap math unit tests (contiguous batching, late
  lookback); E2E on demo workflow with `partitions: daily`.
- Stage 3: automation DSL evaluator truth table; freshness violation
  propagation E2E.

//! End-to-end GROUP BY correctness guardrail for the `core.sql` node
//! (`SqlProxyLogical`, late-bound in `core/engine/planner.rs`).
//!
//! STATUS (2026-11-20, verified on main @ 7eace18): the historically
//! reported string-column GROUP BY collapse (skill swanflow-dev: N groups
//! collapse to a single NULL/empty key) did NOT reproduce in ANY workflow
//! shape on this commit — 6 shape categories, both string encodings
//! (Utf8View from DuckDB reads, Utf8 from DataFusion literals/parquet),
//! multi-stage materialization, the P1-6 fan-out shape, and a 1000-row x
//! 50-key scale run (see `openspec/changes/sql-group-by-correctness/`
//! design.md §"阶段 A 实测" for the full table with real outputs).
//!
//! This test is therefore a PERMANENT GUARDRAIL, not a red repro: it pins
//! the currently-correct behavior on the REAL engine path and will go RED
//! (explicit "COLLAPSE DETECTED" failure) if the historical collapse ever
//! reappears in any of these shapes. It deliberately does NOT use the
//! `MemTable + into_view` mirror (see `core/sql_collapse_probe.rs`) — it
//! drives the same machinery `SwanFlowEngine::execute_workflow` drives per
//! node:
//!
//!   * `WorkflowCompiler::with_cache_dir(true, cache_dir).compile(...)`
//!     (identical to engine/mod.rs:673-682),
//!   * per-level `sync_metadata_to_context` equivalent (engine/mod.rs:158-182,
//!     re-implemented here because the fn is private; byte-identical logic),
//!   * the exact planner stack of engine/mod.rs:708-716:
//!     `DefaultPhysicalPlanner::with_extension_planners([SwanFlowPlanner])`
//!     wrapped in `SwanFlowQueryPlanner`, built via
//!     `SessionStateBuilder::new_from_existing(ctx.state()).with_query_planner(..)`,
//!   * `state.create_physical_plan(&stage_plan.plan)` + `collect` — the same
//!     calls as engine/mod.rs:1151-1158, which is what triggers the
//!     `plan_extension` late-binding for SqlProxy (planner.rs:366-413:
//!     per-proxy scratch SessionContext, `input_{i}` views, `scratch.sql`,
//!     `align_schemas`).
//!
//! Why not call `SwanFlowEngine::execute_workflow` directly: on this commit
//! its P2-15 `'retry_loop` (engine/mod.rs:1149-1497) has NO `break` on the
//! success path, so any workflow that executes at least one node re-runs
//! that node forever after success (second run fails register_table with
//! "The table node_... already exists") and the run never completes. That
//! is a PRE-EXISTING, unrelated defect (reproduced with the repo's own
//! examples/etl_flow.yaml; see design.md §阶段 A 实测, row "default staged
//! run"); this test drives the exact per-node code of that path minus the
//! broken loop.
//!
//! Shape coverage (one test per shape category from tasks.md A2):
//!   1. duckdb_read seed (Utf8View) -> core.sql GROUP BY                [2 levels]
//!   2. duckdb_read -> filter -> core.sql GROUP BY (stage boundary +
//!      intermediate materialization right before the SqlProxy)          [3 levels]
//!   3. fan-out: one source, TWO core.sql GROUP BY nodes in the SAME
//!      level (the P1-6 historical cross-node-mixup shape,
//!      planner.rs:369-378)
//!   4. DataFusion-literal source (Utf8) -> core.sql GROUP BY
//!      (SqlProxy feeding SqlProxy; the input_0 view wraps another
//!      SqlProxy plan)
//!   5. parquet file source (Utf8) -> core.sql GROUP BY
//!   6. scale: 1000 rows / 50 distinct keys
//!
//! Run: `cargo test -p swanflow --test sql_group_by_collapse`
//! (needs DUCKDB_DOWNLOAD_LIB=1 for the first build only).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use swanflow::core::compiler::WorkflowCompiler;
use swanflow::core::engine::{SwanFlowEngine, SwanFlowPlanner, SwanFlowQueryPlanner};
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;

// ── seeds / queries ────────────────────────────────────────────────────────

const SEED_SMALL: &str = "SELECT 'a' AS cat, 1 AS v \
     UNION ALL SELECT 'a', 2 \
     UNION ALL SELECT 'a', 3 \
     UNION ALL SELECT 'b', 4 \
     UNION ALL SELECT 'b', 5 \
     UNION ALL SELECT 'c', 6";

const SEED_BIG: &str = "SELECT 'cat_' || (i % 50) AS cat, i AS v FROM range(1000) t(i)";

const GROUP_BY: &str = "SELECT cat, count(*) AS n FROM input_0 GROUP BY cat ORDER BY cat";

// Expected for SEED_SMALL (6 rows): a=3, b=2, c=1.
const EXPECT_SMALL: [(&str, i64); 3] = [("a", 3), ("b", 2), ("c", 1)];

// ── helpers ────────────────────────────────────────────────────────────────

fn node(id: Uuid, type_id: &str, config: serde_json::Value) -> NodeInstance {
    NodeInstance {
        id,
        type_id: type_id.into(),
        config,
        state: swanflow::nodes::NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    }
}

fn edge() -> EdgeInstance {
    EdgeInstance {
        source_port: 0,
        target_port: 0,
    }
}

fn duckdb_read_node(id: Uuid, query: &str) -> NodeInstance {
    node(
        id,
        "core.io.duckdb_read",
        json!({ "path": ":memory:", "query": query }),
    )
}

fn groupby_node(id: Uuid) -> NodeInstance {
    node(id, "core.sql", json!({ "query": GROUP_BY }))
}

/// Faithful copy of `core::engine::sync_metadata_to_context`
/// (engine/mod.rs:158-182, private to the engine module): register every
/// materialized node table from `flow_metadata` into the shared context.
async fn sync_metadata(ctx: &SessionContext, cache_dir: &str) {
    let metadata_path = format!("{cache_dir}/metadata.duckdb");
    let tables: Vec<(String, String)> = {
        let conn = duckdb::Connection::open(&metadata_path)
            .unwrap_or_else(|e| panic!("open {metadata_path}: {e}"));
        let mut stmt = conn
            .prepare("SELECT table_name, parquet_path FROM flow_metadata")
            .expect("prepare flow_metadata");
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query flow_metadata");
        rows.flatten().collect()
    };
    for (table_name, path) in tables {
        let _ = ctx.deregister_table(&table_name);
        ctx.register_parquet(&table_name, &path, ParquetReadOptions::default())
            .await
            .ok();
    }
}

/// Drive a compiled workflow stage-by-stage exactly the way
/// `SwanFlowEngine::execute_workflow` does per node (engine/mod.rs:828-1265:
/// per-level metadata sync, then per-node create_physical_plan + collect),
/// minus the P2-15 retry loop that hangs this path on the current commit.
/// Returns the collected RecordBatches for every requested target node.
async fn run_staged(workflow: &Workflow, targets: &[Uuid]) -> HashMap<Uuid, Vec<RecordBatch>> {
    let cache_dir = format!("target/sql_group_by_cache_{}", Uuid::new_v4().simple());
    let engine =
        SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.clone());

    let (tx, _rx) = mpsc::unbounded_channel();

    let mut compiler = WorkflowCompiler::with_cache_dir(true, engine.cache_dir.clone());
    compiler.ctx = engine.ctx.clone();
    compiler.set_event_tx(tx);

    let stages = compiler
        .compile(workflow, &engine.registry, Some(engine.ctx.clone()))
        .await
        .unwrap_or_else(|e| panic!("workflow compile failed: {e}"));

    let mut results: HashMap<Uuid, Vec<RecordBatch>> = HashMap::new();

    // Overall guard: if any stage ever re-enters a hang (like the P2-15
    // loop does in the engine), the whole driver must fail loudly instead
    // of blocking the test harness forever.
    tokio::time::timeout(Duration::from_secs(240), async {
        for level in stages {
            // 1. Initial sync for this level (engine/mod.rs:843)
            sync_metadata(engine.ctx.as_ref(), &engine.cache_dir).await;

            // 2. Per-node execution (engine/mod.rs:854-1265, minus the
            //    infinite P2-15 retry loop).
            for stage_plan in level {
                let planner = SwanFlowPlanner {
                    event_tx: mpsc::unbounded_channel().0,
                    ctx: engine.ctx.clone(),
                    cache_dir: engine.cache_dir.clone(),
                };
                let physical_planner =
                    Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
                        Arc::new(planner),
                    ]));
                let state = SessionStateBuilder::new_from_existing(engine.ctx.state())
                    .with_query_planner(Arc::new(SwanFlowQueryPlanner { physical_planner }))
                    .build();

                let physical_plan = state
                    .create_physical_plan(&stage_plan.plan)
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "create_physical_plan failed for node {} ({}): {e}",
                            stage_plan.node_id, stage_plan.node_type
                        )
                    });

                let batches = collect(physical_plan, engine.ctx.task_ctx())
                    .await
                    .unwrap_or_else(|e| {
                        panic!(
                            "collect failed for node {} ({}): {e}",
                            stage_plan.node_id, stage_plan.node_type
                        )
                    });

                if targets.contains(&stage_plan.node_id) {
                    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                    assert!(
                        total > 0,
                        "node {} ({}) produced 0 rows",
                        stage_plan.node_id,
                        stage_plan.node_type
                    );
                    results.insert(stage_plan.node_id, batches);
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "staged run did not complete within 240s — hang regression \
             (cf. P2-15 retry loop, engine/mod.rs:1149)"
        )
    });

    results
}

/// Extract (key, count) tuples from a two-column GROUP BY output, accepting
/// BOTH string encodings (Utf8View = DuckDB read-back form, Utf8 = DataFusion
/// literal / parquet form).
fn group_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in batches {
        assert_eq!(b.num_columns(), 2, "GROUP BY output must be (key, count)");
        let key = b.column(0).as_any();
        let n = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "expected Int64 count column, got {:?}",
                    b.schema().field(1).data_type()
                )
            });
        if let Some(s) = key.downcast_ref::<StringViewArray>() {
            for i in 0..b.num_rows() {
                out.push((s.value(i).to_string(), n.value(i)));
            }
        } else if let Some(s) = key.downcast_ref::<StringArray>() {
            for i in 0..b.num_rows() {
                out.push((s.value(i).to_string(), n.value(i)));
            }
        } else {
            panic!(
                "expected string key column, got {:?}",
                b.schema().field(0).data_type()
            );
        }
    }
    out
}

/// Assert the result is exactly the expected (key, count) tuples AND is not
/// a collapse (single row with an empty/NULL key, or zero rows). On a
/// collapse this fails with an explicit COLLAPSE message — that failure is
/// the red signal tasks.md A3 asks for.
fn assert_grouped(rows: &[(String, i64)], expected: &[(&str, i64)], label: &str) {
    let collapsed = rows.len() <= 1
        && rows
            .iter()
            .any(|(k, _)| k.is_empty() || k.eq_ignore_ascii_case("null"));
    assert!(
        !collapsed,
        "[{label}] GROUP BY COLLAPSE DETECTED: {rows:?} \
         (expected {expected:?})"
    );
    assert_eq!(
        rows,
        expected
            .iter()
            .map(|(k, n)| (k.to_string(), *n))
            .collect::<Vec<_>>(),
        "[{label}] GROUP BY key/count mismatch"
    );
    assert!(
        rows.iter().all(|(k, n)| !k.is_empty() && *n > 0),
        "[{label}] empty key or non-positive count: {rows:?}"
    );
}

// ── shape 1: duckdb_read (Utf8View) -> core.sql GROUP BY ──────────────────

#[tokio::test]
async fn shape1_duckdb_source_groupby() {
    let src = Uuid::new_v4();
    let sql = Uuid::new_v4();
    let mut wf = Workflow::new("sql-groupby-shape1");
    let i_src = wf.add_node(duckdb_read_node(src, SEED_SMALL));
    let i_sql = wf.add_node(groupby_node(sql));
    wf.add_edge(i_src, i_sql, edge()).unwrap();

    let results = run_staged(&wf, &[sql]).await;
    let rows = group_rows(&results[&sql]);
    assert_grouped(&rows, &EXPECT_SMALL, "shape1 duckdb(Utf8View) source");
}

// ── shape 2: multi-stage — filter materializes right before the SqlProxy ─

#[tokio::test]
async fn shape2_multistage_filter_before_groupby() {
    let src = Uuid::new_v4();
    let filter = Uuid::new_v4();
    let sql = Uuid::new_v4();
    let mut wf = Workflow::new("sql-groupby-shape2");
    let i_src = wf.add_node(duckdb_read_node(src, SEED_SMALL));
    let i_filter = wf.add_node(node(
        filter,
        "core.transform.filter",
        json!({ "expr": "v > 0" }),
    ));
    let i_sql = wf.add_node(groupby_node(sql));
    wf.add_edge(i_src, i_filter, edge()).unwrap();
    wf.add_edge(i_filter, i_sql, edge()).unwrap();

    let results = run_staged(&wf, &[sql]).await;
    let rows = group_rows(&results[&sql]);
    assert_grouped(
        &rows,
        &EXPECT_SMALL,
        "shape2 multi-stage (filter before SqlProxy)",
    );
}

// ── shape 3: P1-6 fan-out — two SqlProxy nodes share one upstream ─────────

#[tokio::test]
async fn shape3_fanout_two_sql_same_level() {
    let src = Uuid::new_v4();
    let sql1 = Uuid::new_v4();
    let sql2 = Uuid::new_v4();
    let mut wf = Workflow::new("sql-groupby-shape3");
    let i_src = wf.add_node(duckdb_read_node(src, SEED_SMALL));
    let i_sql1 = wf.add_node(groupby_node(sql1));
    let i_sql2 = wf.add_node(groupby_node(sql2));
    wf.add_edge(i_src, i_sql1, edge()).unwrap();
    wf.add_edge(i_src, i_sql2, edge()).unwrap();

    let results = run_staged(&wf, &[sql1, sql2]).await;
    let r1 = group_rows(&results[&sql1]);
    let r2 = group_rows(&results[&sql2]);
    // Each branch must get the FULL correct grouping of the SHARED upstream
    // — the P1-6 defect mode was cross-node mixup (one branch seeing the
    // other's view) or a collapsed/empty key.
    assert_grouped(&r1, &EXPECT_SMALL, "shape3 fan-out branch 1");
    assert_grouped(&r2, &EXPECT_SMALL, "shape3 fan-out branch 2");
    assert_eq!(
        r1, r2,
        "fan-out branches disagree — input_0 alias mixup (P1-6 defect mode)"
    );
}

// ── shape 4: DataFusion-literal source (Utf8) -> SqlProxy (proxy→proxy) ───

#[tokio::test]
async fn shape4_literal_utf8_sqlproxy_source() {
    let src = Uuid::new_v4();
    let sql = Uuid::new_v4();
    let mut wf = Workflow::new("sql-groupby-shape4");
    let i_src = wf.add_node(node(src, "core.sql", json!({ "query": SEED_SMALL })));
    let i_sql = wf.add_node(groupby_node(sql));
    wf.add_edge(i_src, i_sql, edge()).unwrap();

    let results = run_staged(&wf, &[sql]).await;
    let rows = group_rows(&results[&sql]);
    assert_grouped(&rows, &EXPECT_SMALL, "shape4 literal(Utf8) SqlProxy source");
}

// ── shape 5: parquet file source (Utf8) -> core.sql GROUP BY ──────────────

#[tokio::test]
async fn shape5_parquet_utf8_source() {
    // Write a real parquet seed file: cat (Utf8) / v (Int32).
    let path = format!(
        "target/sql_group_by_seed_{}.parquet",
        Uuid::new_v4().simple()
    );
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cat", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b", "c"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let src = Uuid::new_v4();
    let sql = Uuid::new_v4();
    let mut wf = Workflow::new("sql-groupby-shape5");
    let i_src = wf.add_node(node(src, "core.io.parquet_read", json!({ "path": path })));
    let i_sql = wf.add_node(groupby_node(sql));
    wf.add_edge(i_src, i_sql, edge()).unwrap();

    let results = run_staged(&wf, &[sql]).await;
    let rows = group_rows(&results[&sql]);
    assert_grouped(&rows, &EXPECT_SMALL, "shape5 parquet(Utf8) source");

    let _ = std::fs::remove_file(&path);
}

// ── shape 6: scale — 1000 rows / 50 distinct keys ─────────────────────────

#[tokio::test]
async fn shape6_big_1000_rows_50_keys() {
    let src = Uuid::new_v4();
    let sql = Uuid::new_v4();
    let mut wf = Workflow::new("sql-groupby-shape6");
    let i_src = wf.add_node(duckdb_read_node(src, SEED_BIG));
    let i_sql = wf.add_node(groupby_node(sql));
    wf.add_edge(i_src, i_sql, edge()).unwrap();

    let results = run_staged(&wf, &[sql]).await;
    let rows = group_rows(&results[&sql]);

    let total: i64 = rows.iter().map(|(_, n)| *n).sum();
    assert_eq!(
        rows.len(),
        50,
        "expected 50 groups, got {} ({rows:?})",
        rows.len()
    );
    assert_eq!(
        total, 1000,
        "row count must be preserved (1000), got {total}"
    );
    assert!(
        rows.iter().all(|(_, n)| *n == 20),
        "every key must have exactly 20 rows: {rows:?}"
    );
    let mut keys: Vec<String> = rows.iter().map(|(k, _)| k.clone()).collect();
    keys.sort();
    let mut expected: Vec<String> = (0..50).map(|i| format!("cat_{i}")).collect();
    expected.sort(); // lexicographic, same order as `keys`
    assert_eq!(keys, expected, "key set mismatch");
}

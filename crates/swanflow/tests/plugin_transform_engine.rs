//! The engine-seat test: a plugin package driven *by the engine*, not by hand.
//!
//! `plugin_sort_equivalence.rs` proves the host runs a plugin correctly. It
//! cannot prove the engine can schedule one — that was the actual gap, because
//! `NodeBehavior::propose_plan` speaks `LogicalPlan` and a plugin only speaks
//! bytes. These tests go through `propose_plan` → `SwanFlowPlanner` →
//! `ExecutionPlan`, so a missing extension-planner arm fails here instead of in
//! a user's pipeline.
//!
//! Build the example plugin first:
//!
//! ```text
//! plugins/examples/sort/build.sh
//! cargo test -p swanflow --features wasm --test plugin_transform_engine
//! ```

#![cfg(feature = "wasm")]

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::prelude::SessionContext;
use serde_json::json;

use swanflow::core::engine::{SwanFlowPlanner, SwanFlowQueryPlanner};
use swanflow::nodes::registry::NodeRegistry;

fn plugin_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("plugins")
        .join("examples")
        .join("sort")
}

fn require_plugin() -> String {
    let dir = plugin_dir();
    assert!(
        dir.join("sort.wasm").exists(),
        "missing {} — build the plugin first: plugins/examples/sort/build.sh",
        dir.join("sort.wasm").display()
    );
    dir.to_str().unwrap().to_string()
}

const AMOUNTS: [i64; 6] = [30, 5, 100, 5, 2, 77];
const IDS: [i64; 6] = [1, 2, 3, 4, 5, 6];

async fn ctx_with_table() -> SessionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(AMOUNTS.to_vec())),
            Arc::new(Int64Array::from(IDS.to_vec())),
        ],
    )
    .expect("input batch");
    let mem = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(mem)).expect("register");
    ctx
}

/// Run a logical plan through the same planner stack the engine uses.
async fn execute_with_engine(
    ctx: &SessionContext,
    logical: datafusion::logical_expr::LogicalPlan,
) -> datafusion::error::Result<Vec<RecordBatch>> {
    let planner = SwanFlowPlanner {
        event_tx: tokio::sync::mpsc::unbounded_channel().0,
        ctx: Arc::new(ctx.clone()),
        cache_dir: std::env::temp_dir().to_string_lossy().to_string(),
    };
    let physical_planner = Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
        Arc::new(planner),
    ]));
    let state = SessionStateBuilder::new_from_existing(ctx.state())
        .with_query_planner(Arc::new(SwanFlowQueryPlanner { physical_planner }))
        .build();
    let physical = state.create_physical_plan(&logical).await?;
    datafusion::physical_plan::collect(physical, ctx.task_ctx()).await
}

fn registry() -> NodeRegistry {
    // `new()` is the empty shell; `default()` is what calls `register_core_nodes`.
    NodeRegistry::default()
}

#[tokio::test]
async fn the_registry_registers_the_node() {
    let node = registry()
        .get_node("core.plugin.wasm_transform")
        .expect("core.plugin.wasm_transform must be registered with --features wasm");
    assert_eq!(node.spec().type_id, "core.plugin.wasm_transform");
}

#[tokio::test]
async fn a_plugin_transform_is_scheduled_by_the_engine_and_matches_sql() {
    let dir = require_plugin();
    let ctx = ctx_with_table().await;
    let input = ctx
        .table("t")
        .await
        .expect("table")
        .into_optimized_plan()
        .expect("logical plan");

    let node = registry()
        .get_node("core.plugin.wasm_transform")
        .expect("node");
    let cfg = json!({
        "plugin_dir": dir,
        "plugin": "sort",
        "function": "sort_table",
    });
    let planned = node
        .propose_plan(vec![input], &cfg)
        .await
        .expect("propose_plan");

    let batches = execute_with_engine(&ctx, planned[0].clone())
        .await
        .expect("the engine must be able to schedule a plugin transform");
    assert_eq!(batches.len(), 1);

    let got = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .values()
        .to_vec();

    // Ground truth from SQL on the same rows, not from the plugin agreeing with
    // itself. Must be the engine's answer, so run it through the engine's ctx.
    let sql = ctx
        .sql("SELECT amount, id FROM t ORDER BY amount ASC")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("sql rows");
    let want = sql[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64")
        .values()
        .to_vec();

    assert_eq!(got, want, "the plugin's row order must match SQL ORDER BY");
    assert_ne!(
        got,
        AMOUNTS.to_vec(),
        "sanity: the input was unsorted, so the assertion above means something"
    );
}

#[tokio::test]
async fn a_missing_plugin_fails_at_plan_time_by_name() {
    let node = registry()
        .get_node("core.plugin.wasm_transform")
        .expect("node");
    let ctx = ctx_with_table().await;
    let input = ctx
        .table("t")
        .await
        .unwrap()
        .into_optimized_plan()
        .expect("logical plan");
    let cfg = json!({
        "plugin_dir": plugin_dir().to_str().unwrap(),
        "plugin": "no-such-plugin",
    });
    let err = node
        .propose_plan(vec![input], &cfg)
        .await
        .expect_err("a plugin that is not there must not plan");
    let msg = err.to_string();
    assert!(
        msg.contains("no-such-plugin"),
        "the error must name the plugin, got: {msg}"
    );
}

#[tokio::test]
async fn a_schema_that_does_not_match_the_plugin_output_is_reported() {
    let dir = require_plugin();
    let ctx = ctx_with_table().await;
    let input = ctx
        .table("t")
        .await
        .unwrap()
        .into_optimized_plan()
        .expect("logical plan");

    let node = registry()
        .get_node("core.plugin.wasm_transform")
        .expect("node");
    // Deliberately wrong: sort keeps both columns, so declaring one bogus
    // column must be caught at run time rather than silently accepted.
    let cfg = json!({
        "plugin_dir": dir,
        "plugin": "sort",
        "function": "sort_table",
        "output_schema": [{"name": "only_column", "type": "int64"}],
    });
    let planned = node
        .propose_plan(vec![input], &cfg)
        .await
        .expect("propose_plan");

    let err = execute_with_engine(&ctx, planned[0].clone())
        .await
        .expect_err("a declared schema the plugin does not produce must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("declares") || msg.contains("schema"),
        "the error must point at the schema contract, got: {msg}"
    );
}

#[tokio::test]
async fn core_script_wasm_now_lowers_to_a_physical_plan() {
    // Regression guard for a node that was registered and planned but never
    // lowered: every workflow using it died with "No physical plan found for
    // user defined logical node: WasmUdf". Planning is the whole assertion —
    // executing needs a module that exports `transform`, which is a different
    // fixture.
    let node = registry().get_node("core.script.wasm").expect("node");
    let ctx = ctx_with_table().await;
    let input = ctx
        .table("t")
        .await
        .unwrap()
        .into_optimized_plan()
        .expect("logical plan");
    let cfg = json!({ "module": "/nonexistent/does-not-matter-at-plan-time.wasm" });
    let planned = node
        .propose_plan(vec![input], &cfg)
        .await
        .expect("propose_plan");

    let planner = SwanFlowPlanner {
        event_tx: tokio::sync::mpsc::unbounded_channel().0,
        ctx: Arc::new(ctx.clone()),
        cache_dir: std::env::temp_dir().to_string_lossy().to_string(),
    };
    let physical_planner = Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
        Arc::new(planner),
    ]));
    let state = SessionStateBuilder::new_from_existing(ctx.state())
        .with_query_planner(Arc::new(SwanFlowQueryPlanner { physical_planner }))
        .build();
    let physical = state.create_physical_plan(&planned[0]).await;
    assert!(
        physical.is_ok(),
        "core.script.wasm must lower to an ExecutionPlan, got: {:?}",
        physical.err()
    );
}

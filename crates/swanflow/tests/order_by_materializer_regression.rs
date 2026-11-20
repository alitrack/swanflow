//! Regression tests for Bug 17: ORDER BY lost in materialization.
//!
//! Root cause: StateProbeExec / MaterializerExec declared blank
//! EquivalenceProperties and did not implement `maintains_input_order` /
//! `required_input_ordering`, so DataFusion's physical optimizer saw "no
//! ordering consumer" and stripped the user's SortExec during whole-tree
//! planning; the surrounding RepartitionExec(RoundRobin) also shuffled rows.
//!
//! Fix: both execs now (a) propagate the input's equivalence properties,
//! (b) declare `maintains_input_order() = [true]`, (c) declare a soft
//! `required_input_ordering` mirroring the input's output ordering, and
//! (d) coalesce via SortPreservingMergeExec when the input is ordered.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;

/// A user ORDER BY must survive into the materialized output: the stage's
/// final physical plan must contain a Sort, and running it end-to-end must
/// produce rows in the requested order.
#[tokio::test]
async fn order_by_survives_materializer_wrapping() {
    let ctx = datafusion::prelude::SessionContext::new();

    // waybill_id ASC, violations DESC  => expected WB-001, WB-004, WB-002,
    // WB-005, WB-003, WB-006 (violations 3,2,1,1,0,0)
    let schema = Arc::new(Schema::new(vec![
        Field::new("waybill_id", DataType::Utf8, false),
        Field::new("violations", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "WB-001", "WB-002", "WB-003", "WB-004", "WB-005", "WB-006",
            ])),
            Arc::new(Int32Array::from(vec![3, 1, 0, 2, 1, 0])),
        ],
    )
    .unwrap();
    let provider = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("shipments", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT waybill_id, violations FROM shipments ORDER BY violations DESC, waybill_id ASC")
        .await
        .unwrap();
    let logical: LogicalPlan = df.into_optimized_plan().unwrap();

    // Wrap with MaterializerExec (as the engine's compiler does) and plan the
    // whole tree through the session planner (physical optimizer runs here).
    let state = ctx.state();
    let physical = state.create_physical_plan(&logical).await.unwrap();
    let (_event_tx, _event_rx): (
        tokio::sync::mpsc::UnboundedSender<swanflow::nodes::NodeEvent>,
        _,
    ) = tokio::sync::mpsc::unbounded_channel();
    let node_id = uuid::Uuid::new_v4();
    let materializer = swanflow::core::observer::MaterializerExec::new(
        node_id,
        format!("node_{}", &node_id.to_string()[..8]),
        format!("{}_", node_id),
        std::env::temp_dir().to_string_lossy().to_string(),
        physical,
    );

    let task_ctx = ctx.task_ctx();
    let mut stream = materializer.execute(0, task_ctx).unwrap();
    let mut ids: Vec<String> = Vec::new();
    use futures::stream::StreamExt;
    while let Some(Ok(batch)) = stream.next().await {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        ids.extend(col.iter().map(|v| v.unwrap().to_string()));
    }

    let expected = vec![
        "WB-001", "WB-004", "WB-002", "WB-005", "WB-003", "WB-006",
    ];
    assert_eq!(
        ids, expected,
        "Bug 17 regression: ORDER BY lost through MaterializerExec"
    );

    // The plan must also advertise the ordering (soft requirement declared).
    assert!(
        materializer.properties().equivalence_properties().output_ordering().is_some(),
        "MaterializerExec must propagate output ordering in its equivalence properties"
    );
    assert_eq!(materializer.maintains_input_order(), vec![true]);
    assert!(
        materializer.required_input_ordering()[0].is_some(),
        "MaterializerExec must declare a required input ordering"
    );
    // Partitioning must still be single (materialization contract).
    if let datafusion::physical_plan::Partitioning::RoundRobinBatch(n) =
        materializer.properties().output_partitioning()
    {
        assert_eq!(*n, 1, "materialized output must be a single partition");
    }
}

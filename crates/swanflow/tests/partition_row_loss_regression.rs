// Regression: multi-partition inputs (UnionExec) must not lose rows through
// StateProbeExec / MaterializerExec. Previously both declared 1 output partition
// but only pulled input partition 0 — a 3-branch UNION ALL silently materialized
// 1 row instead of 3.
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::*;

#[tokio::test]
async fn union_all_rows_survive_state_probe_and_materializer_partition_pull() {
    let ctx = SessionContext::new();
    let q = "SELECT 'a' AS v UNION ALL SELECT 'b' UNION ALL SELECT 'c'";
    let df = ctx.sql(q).await.unwrap();
    let plan = df.into_optimized_plan().unwrap();
    let phys = ctx.state().create_physical_plan(&plan).await.unwrap();
    // Simulate what swanflow engine does: wrap with coalesce before single-partition pull.
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(phys));
    assert_eq!(coalesced.properties().output_partitioning().partition_count(), 1);
    let batches = collect(coalesced, ctx.task_ctx()).await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "UNION ALL must not lose rows across partitions");
}

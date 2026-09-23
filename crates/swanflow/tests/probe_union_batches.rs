// Probe: how many RecordBatches and rows does DataFusion emit for a 3-row UNION ALL
// through the exact swanflow path (ctx.sql -> into_optimized_plan -> physical -> collect)?
use datafusion::prelude::*;

#[tokio::test]
async fn union_all_batch_count_probe() {
    let ctx = SessionContext::new();
    let q = "SELECT 'Alice works for ACME Corp.' as input UNION ALL SELECT 'ACME Corp SF.' UNION ALL SELECT 'Bob CTO ACME.'";
    let df = ctx.sql(q).await.unwrap();
    let plan = df.into_optimized_plan().unwrap();
    let state = ctx.state();
    let phys = state.create_physical_plan(&plan).await.unwrap();
    println!("PHYSICAL_PLAN: {}", displayable(phys.as_ref()).indent(true));
    let batches = datafusion::physical_plan::collect(phys, state.task_ctx()).await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("BATCH_COUNT={} TOTAL_ROWS={}", batches.len(), total_rows);
    for (i, b) in batches.iter().enumerate() {
        println!("  batch[{i}] rows={}", b.num_rows());
    }
    assert_eq!(total_rows, 3, "must not lose UNION ALL rows");
}

use datafusion::physical_plan::displayable;

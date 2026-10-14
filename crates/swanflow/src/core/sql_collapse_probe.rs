//! Guardrail + probe for `core.sql` GROUP BY correctness. Run via
//! `cargo test -p swanflow --lib sql_collapse_probe`.
//!
//! Historical claim (skill swanflow-dev): a string-column GROUP BY in
//! `core.sql` collapses to a single NULL row. This test mirrors the
//! planner's late-binding shape (planner.rs:341-365 — input plan registered
//! as a VIEW under `input_0`, then `ctx.sql` against the shared context) and
//! asserts GROUP BY partitions correctly for BOTH a string (Utf8View) column
//! and an integer column: 3 groups (a=2,b=2,c=1), never a single collapsed
//! NULL row.
//!
//! STATUS (2026-10-14): the isolated MemTable+view shape does NOT reproduce
//! the collapse (string and int both group correctly). The collapse is
//! therefore workflow-shape-dependent (stage materialization / shared-ctx
//! re-registration), not a bare string-column property. The change
//! `sql-group-by-correctness` pins the triggering condition at workflow
//! level before the fix is considered done. This test stays as the
//! unit-level guardrail and is promoted to a permanent regression test on
//! fix-land (drop `#[cfg(test)]` gating if it ever leaves the probe module).

use datafusion::arrow::array::{Int32Array, Int64Array, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{DataFrame, SessionContext};
use std::sync::Arc;

async fn run_groupby(group_col: &str) -> Vec<(String, i32)> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8View, true),
        Field::new("region", DataType::Int32, true),
    ]));
    let cat = Arc::new(StringViewArray::from(vec!["a", "a", "b", "b", "c"]));
    let region = Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3]));
    let batch = RecordBatch::try_new(schema.clone(), vec![cat, region]).unwrap();

    let mem = Arc::new(
        datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
    );

    // Mirror the planner's late-binding shape (planner.rs:341-358): the input
    // logical plan is registered as a VIEW under `input_0`. We build that
    // input plan via ctx.table("base").into_optimized_plan() and wrap it in a
    // DataFrame -> into_view(), exactly as the planner does for a real node.
    let ctx = SessionContext::new();
    ctx.register_table("base", mem.clone()).unwrap();
    let base_plan = ctx.table("base").await.unwrap().into_optimized_plan().unwrap();
    let df = DataFrame::new(ctx.state(), base_plan);
    ctx.register_table("input_0", df.into_view()).unwrap();

    let out = ctx
        .sql(&format!(
            "SELECT {group_col}, count(*) AS n FROM input_0 GROUP BY {group_col} ORDER BY {group_col}"
        ))
        .await
        .unwrap();
    let res = out.collect().await.unwrap();
    let mut rows = Vec::new();
    for b in &res {
        let n: &Int64Array = b.column(1).as_any().downcast_ref().unwrap();
        if group_col == "category" {
            let c: &StringViewArray = b.column(0).as_any().downcast_ref().unwrap();
            for i in 0..b.num_rows() {
                rows.push((c.value(i).to_string(), n.value(i) as i32));
            }
        } else {
            let r: &Int32Array = b.column(0).as_any().downcast_ref().unwrap();
            for i in 0..b.num_rows() {
                rows.push((r.value(i).to_string(), n.value(i) as i32));
            }
        }
    }
    rows
}

#[tokio::test]
async fn probe_string_vs_int_groupby() {
    let s = run_groupby("category").await;
    let i = run_groupby("region").await;
    eprintln!("\n[PROBE] GROUP BY string(category) -> {} group(s): {:?}", s.len(), s);
    eprintln!("[PROBE] GROUP BY int(region)     -> {} group(s): {:?}", i.len(), i);

    // Guardrail: BOTH the string (Utf8View) and integer columns must
    // partition into exactly 3 groups with the expected counts. A collapse to
    // a single NULL/empty-key row (the historical `core.sql` bug) trips this.
    assert_eq!(i.len(), 3, "int GROUP BY sanity (3 groups) failed, got {i:?}");
    assert_eq!(
        s.len(),
        3,
        "string GROUP BY collapsed or mis-grouped (want 3 groups a=2,b=2,c=1), got {s:?}"
    );
    assert!(
        !s.iter().any(|(c, _)| c.is_empty()),
        "string GROUP BY produced an empty/NULL key (collapsed): {s:?}"
    );
    let total: i32 = s.iter().map(|(_, n)| *n).sum();
    assert_eq!(total, 5, "string GROUP BY lost rows (want 5 total), got {total}");
}

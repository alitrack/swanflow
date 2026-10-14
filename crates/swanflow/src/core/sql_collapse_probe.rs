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
//! STATUS (2026-10-14, corrected after codex review): this isolated
//! MemTable+view shape does NOT reproduce the historical collapse — string
//! (Utf8View) and integer GROUP BY both partition correctly here. That does
//! NOT prove the collapse is workflow-shape-dependent: this fixture bypasses
//! `SqlProxyLogical` metadata sync, stage materialization, and the shared-ctx
//! `input_0` re-registration, which are where real NULL-producing aggregation
//! defects could surface. Five rows cannot exclude a data-dependent defect.
//! The `sql-group-by-correctness` change keeps the competing hypotheses open
//! and will pin the triggering condition at workflow level (A1-A3) before the
//! fix is considered done. This test stays as the unit-level guardrail.

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

    // Guardrail: BOTH the string (Utf8View) and integer columns must partition
    // into the EXACT ordered key/count tuples (a=2,b=2,c=1 and 1=2,2=2,3=1)
    // with no NULL/empty keys. Asserting only on the group count would let a
    // wrong-but-plausible distribution (e.g. counts (1,1,3) over keys a,b,c)
    // or a single NULL-key collapse pass.
    let s_expect: Vec<(&str, i32)> = vec![("a", 2), ("b", 2), ("c", 1)];
    let i_expect: Vec<(&str, i32)> = vec![("1", 2), ("2", 2), ("3", 1)];

    assert_eq!(
        s,
        s_expect
            .iter()
            .map(|(k, n)| (k.to_string(), *n))
            .collect::<Vec<_>>(),
        "string GROUP BY must be exactly (a=2,b=2,c=1); got {s:?}"
    );
    assert_eq!(
        i,
        i_expect
            .iter()
            .map(|(k, n)| (k.to_string(), *n))
            .collect::<Vec<_>>(),
        "int GROUP BY must be exactly (1=2,2=2,3=1); got {i:?}"
    );
    // No NULL/empty keys in either column (a collapse would yield a single
    // empty-string key).
    assert!(
        !s.iter().any(|(k, _)| k.is_empty()),
        "string GROUP BY produced a NULL/empty key: {s:?}"
    );
    assert!(
        !i.iter().any(|(k, _)| k.is_empty()),
        "int GROUP BY produced a NULL/empty key: {i:?}"
    );
    // Row counts are non-null and the total is preserved (5 input rows).
    assert!(s.iter().all(|(_, n)| *n > 0), "string GROUP BY has a zero/null count: {s:?}");
    assert!(i.iter().all(|(_, n)| *n > 0), "int GROUP BY has a zero/null count: {i:?}");
    let s_total: i32 = s.iter().map(|(_, n)| *n).sum();
    let i_total: i32 = i.iter().map(|(_, n)| *n).sum();
    assert_eq!(s_total, 5, "string GROUP BY lost rows (want 5 total), got {s_total}");
    assert_eq!(i_total, 5, "int GROUP BY lost rows (want 5 total), got {i_total}");
}

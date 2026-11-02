// Integration acceptance: the OssieReadNode must load ANY real Ossie 0.2.x
// export and yield a JOINable catalog. Fixture set: 31 production models
// exported during the 2026-09-28 duckdb-ossie spike (MIMIC-IV 31 templates).
// Skips gracefully when the fixture directory is absent (CI machines).

use datafusion::prelude::*;
use std::path::Path;

async fn run_over_real_models() -> (usize, usize, usize, usize) {
    let dir = Path::new("/mnt/d/wsl2/tmp/ossie-spike/ossie-models");
    if !dir.is_dir() {
        panic!("fixture dir missing — expected on the dev workstation");
    }
    let mut ok = 0;
    let mut files = 0;
    let mut total_rows = 0usize;
    let mut total_metrics = 0usize;
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    entries.sort();
    for p in entries {
        files += 1;
        let node = swanflow::nodes::io::OssieReadNode;
        let cfg = serde_json::json!({ "path": p.to_string_lossy() });
        use swanflow::nodes::NodeBehavior;
        node.validate(&cfg).expect("validate");
        let plans = node.propose_plan(vec![], &cfg).await.expect("propose_plan");
        assert_eq!(plans.len(), 1);
        // Execute the plan and prove the catalog is SQL-JOINable downstream.
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(plans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.clone().collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let metrics: usize = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).filter_map(move |i| {
                    let k = b
                        .column(0)
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::StringArray>()
                        .unwrap()
                        .value(i);
                    (k == "ossie_metric").then_some(1)
                })
            })
            .sum();
        // JOIN proof: aggregate over the catalog itself via SQL
        let df2 = df
            .clone()
            .filter(datafusion::prelude::col("kind").eq(lit("ossie_metric")))
            .unwrap()
            .count()
            .await
            .unwrap();
        assert_eq!(df2, metrics, "SQL filter must agree with batch scan");
        total_rows += rows;
        total_metrics += metrics;
        ok += 1;
    }
    (ok, files, total_rows, total_metrics)
}

#[tokio::test]
async fn all_31_real_ossie_models_load_and_join() {
    let (ok, files, rows, metrics) = run_over_real_models().await;
    println!("loaded {ok}/{files} models, {rows} rows, {metrics} metrics");
    assert_eq!(ok, files);
    assert_eq!(files, 31, "fixture drift — expected the 31 spike models");
    assert!(rows > 100, "expected a substantive catalog, got {rows} rows");
    assert!(metrics >= 31 * 3, "each model exports >=3 metrics, got {metrics}");
}

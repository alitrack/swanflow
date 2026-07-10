/// E2E test: KNIME workflow → SwanFlow conversion → DuckDB SQL execution
use swanflow_core::kernel::mock::MockDb;
use swanflow_core::kernel::DbWrite;
use swanflow_knime::{KnimeNodeMapper, KnwfParser};

#[tokio::test]
async fn test_e2e_convert_and_execute_sql() {
    let wfs = swanflow_knime::parse_knar("/tmp/KNIME_study.knar")
        .expect("Failed to parse .knar");
    
    let mapper = KnimeNodeMapper::new();
    let db = MockDb::new();
    
    let mut total_sql = 0usize;
    let mut ok_sql = 0usize;
    
    for (name, knwf) in wfs.iter().take(3) {
        let sw_wf = match mapper.convert(knwf.clone()).await {
            Ok(w) => w,
            Err(e) => {
                eprintln!("Convert {} failed: {}", name, e);
                continue;
            }
        };
        
        for idx in sw_wf.graph.node_indices() {
            let node = &sw_wf.graph[idx];
            if node.type_id == "core.sql" {
                if let Some(q) = node.config.get("query").and_then(|v| v.as_str()) {
                    if !q.contains("input_") {
                        total_sql += 1;
                        match db.execute(q).await {
                            Ok(_) => ok_sql += 1,
                            Err(e) => eprintln!("  SQL FAIL: {} — {}", &q[..q.len().min(80)], e),
                        }
                    }
                }
            }
        }
    }
    
    println!("SQL execution: {}/{} passed", ok_sql, total_sql);
    assert!(total_sql > 0, "No SQL nodes found to test");
    assert!(ok_sql > 0, "All SQL executions failed");
}

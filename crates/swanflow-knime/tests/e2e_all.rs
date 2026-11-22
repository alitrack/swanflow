/// E2E: all 148 KNIME workflows → SwanFlow → DuckDB SQL execution
use swanflow_core::kernel::mock::MockDb;
use swanflow_core::kernel::DbWrite;

#[tokio::test]
async fn test_e2e_all_workflows() {
    let wfs = swanflow_knime::parse_knar("/tmp/KNIME_study.knar").expect("Failed to parse .knar");

    let mapper = swanflow_knime::KnimeNodeMapper::new();
    let db = MockDb::new();

    let mut total_wf = 0usize;
    let mut ok_wf = 0usize;
    let mut total_sql = 0usize;
    let mut ok_sql = 0usize;
    let mut total_nodes = 0usize;
    let mut mapped_nodes = 0usize;

    for (name, knwf) in &wfs {
        total_wf += 1;
        total_nodes += knwf.nodes.len();

        let sw_wf = match mapper.convert(knwf.clone()).await {
            Ok(w) => w,
            Err(e) => {
                eprintln!("  ❌ {} — convert: {}", name, e);
                continue;
            }
        };

        let mut wf_ok = true;
        let mut wf_sql = 0usize;
        let mut wf_sql_ok = 0usize;

        for idx in sw_wf.graph.node_indices() {
            let node = &sw_wf.graph[idx];
            if node.type_id != "core.missing.placeholder" && node.type_id != "core.util.unknown" {
                mapped_nodes += 1;
            }
            if node.type_id == "core.sql" {
                if let Some(q) = node.config.get("query").and_then(|v| v.as_str()) {
                    if !q.contains("input_") && !q.is_empty() {
                        total_sql += 1;
                        wf_sql += 1;
                        match db.execute(q).await {
                            Ok(_) => {
                                ok_sql += 1;
                                wf_sql_ok += 1;
                            }
                            Err(e) => {
                                wf_ok = false;
                                eprintln!("  SQL [{}] {} — {}", name, &q[..q.len().min(60)], e);
                            }
                        }
                    }
                }
            }
        }

        if wf_ok {
            ok_wf += 1;
        }
    }

    println!("Workflows:  {}/{} converted", ok_wf, total_wf);
    println!(
        "Nodes:      {}/{} mapped ({:.1}%)",
        mapped_nodes,
        total_nodes,
        mapped_nodes as f64 / total_nodes as f64 * 100.0
    );
    println!("SQL:        {}/{} executed", ok_sql, total_sql);

    assert!(ok_wf > total_wf / 2, "Less than 50% workflows converted");
    assert!(ok_sql > 0, "No SQL executed");
}

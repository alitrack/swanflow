use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;
use std::fs;
use datafusion::dataframe::DataFrameWriteOptions;

#[tokio::test]
async fn test_universal_read_parquet() {
    let cache_dir = "target/universal_read_test_parquet";
    let engine = SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());
    
    // 1. Create a dummy parquet file
    let parquet_path = format!("{}/data.parquet", cache_dir);
    fs::create_dir_all(cache_dir).unwrap();
    
    let ctx = datafusion::prelude::SessionContext::new();
    let df = ctx.sql("SELECT 1 as id, 'Alice' as name").await.unwrap();
    df.write_parquet(&parquet_path, DataFrameWriteOptions::default(), None).await.unwrap();

    // 2. Build Workflow
    let mut workflow = Workflow::new("Universal Read Parquet");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.io.universal_read".into(),
        config: json!({ "path": parquet_path }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // 3. Execute
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let rustflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    // 4. Verify
    let table_name = format!("node_{}", &node_id.to_string()[..8]);
    let results = engine.ctx.sql(&format!("SELECT * FROM {}", table_name)).await.unwrap().collect().await.unwrap();
    assert_eq!(results[0].num_rows(), 1);
}

#[tokio::test]
async fn test_universal_read_csv() {
    let cache_dir = "target/universal_read_test_csv_single";
    let engine = SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());
    
    // 1. Create a dummy CSV file
    let csv_path = format!("{}/data.csv", cache_dir);
    fs::create_dir_all(cache_dir).unwrap();
    fs::write(&csv_path, "id,name\n1,Alice\n2,Bob").unwrap();

    // 2. Build Workflow
    let mut workflow = Workflow::new("Universal Read CSV Single");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.io.universal_read".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // 3. Execute
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let rustflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    // 4. Verify
    let table_name = format!("node_{}", &node_id.to_string()[..8]);
    let results = engine.ctx.sql(&format!("SELECT * FROM {}", table_name)).await.unwrap().collect().await.unwrap();
    assert_eq!(results[0].num_rows(), 2);
}

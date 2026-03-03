use anyhow::Result;
use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{Workflow, NodeInstance};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_mcp_node_integration() -> Result<()> {
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    let mut workflow = Workflow::new("mcp_test");
    let node_id = Uuid::new_v4();
    let node_id_str = node_id.to_string()[..8].to_string();
    let table_name = format!("node_{}", node_id_str);
    
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.ai.mcp_invoke".to_string(),
        config: json!({
            "tool_name": "echo",
            "arguments": {"msg": "integration test"},
            "server_command": "python3",
            "server_args": ["scripts/mock_mcp_server.py"]
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    
    let mut materialized_name = None;
    let mut parquet_file = None;
    // Wait for the workflow to finish processing events
    while let Some(event) = rx.recv().await {
        println!("DEBUG: [Test] Received Event: {:?}", event);
        if let rustflow::nodes::NodeEvent::Materialized { table_name, parquet_path, .. } = event {
            materialized_name = Some(table_name);
            if !parquet_path.is_empty() {
                parquet_file = Some(parquet_path);
            }
        }
    }
    
    let table_name = materialized_name.expect("Materialized event missing");
    println!("Querying table: {}", table_name);
    
    // Retry logic for querying the table or file
    let mut batch = None;
    for i in 0..10 {
        if let Ok(df) = engine.ctx.table(&table_name).await {
            if let Ok(results) = df.collect().await {
                if !results.is_empty() {
                    println!("Found data in table on attempt {}", i);
                    batch = Some(results[0].clone());
                    break;
                }
            }
        }
        
        // Try reading parquet if table query fails
        if let Some(path) = &parquet_file {
            let path_buf = std::path::Path::new(path);
            println!("Checking parquet path (attempt {}): {} (exists: {})", i, path, path_buf.exists());
            if path_buf.exists() {
                if let Ok(df) = engine.ctx.read_parquet(path, datafusion::prelude::ParquetReadOptions::default()).await {
                    if let Ok(results) = df.collect().await {
                        if !results.is_empty() {
                            println!("Found data in parquet on attempt {}", i);
                            batch = Some(results[0].clone());
                            break;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    let batch = batch.expect("Materialized table should contain results");
    println!("Batch columns: {:?}", batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    
    let content_col = batch
        .column_by_name("content")
        .expect("Column 'content' not found in result batch");
    
    let value = if let Some(a) = content_col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>() {
        a.value(0).to_string()
    } else if let Some(a) = content_col.as_any().downcast_ref::<datafusion::arrow::array::LargeStringArray>() {
        a.value(0).to_string()
    } else if let Some(a) = content_col.as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>() {
        a.value(0).to_string()
    } else {
        panic!("Failed to downcast 'content' to StringArray, LargeStringArray or StringViewArray. Actual type: {:?}", content_col.data_type());
    };
    
    assert_eq!(value, "Echo: integration test");

    Ok(())
}

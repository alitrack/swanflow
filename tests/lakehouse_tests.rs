use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::{NodeEvent, NodeState};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_decoupled_sql_querying() {
    let registry = Arc::new(NodeRegistry::default());
    let cache_dir = "target/lakehouse_test_decoupled";
    let engine = SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());

    // 1. Build a workflow with two disconnected paths
    let mut workflow = Workflow::new("Decoupled SQL Test");

    let node_a_id = Uuid::new_v4();
    let _node_a_idx = workflow.add_node(NodeInstance {
        id: node_a_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT * FROM (VALUES ('Alice', 30), ('Bob', 25)) as t(name, age)" }),
        state: NodeState::Configured,
        materialize: true, 
        sub_workflow: None,
    });

    let table_name = format!("node_{}", &node_a_id.to_string()[..8]);

    let node_b_id = Uuid::new_v4();
    let _node_b_idx = workflow.add_node(NodeInstance {
        id: node_b_id,
        type_id: "core.sql".into(),
        config: json!({ "query": format!("SELECT * FROM {} WHERE age > 28", table_name) }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // 2. Execute Workflow
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Error { node_id, message } = event {
            panic!("Workflow failed at node {}: {}", node_id, message);
        }
    }

    // Wait a bit more for final registrations
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
// 3. Verification
let results_b = engine.ctx.sql(&format!("SELECT * FROM node_{}", &node_b_id.to_string()[..8])).await.unwrap().collect().await.unwrap();
assert_eq!(results_b[0].num_rows(), 1);

println!("DEBUG: Result B Schema: {:?}", results_b[0].schema());
println!("DEBUG: Result B Column 0 Type: {:?}", results_b[0].column(0).data_type());

let name_col = results_b[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringArray>()
    .map(|a| a.value(0).to_string())
    .or_else(|| {
        results_b[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>()
            .map(|a| a.value(0).to_string())
    })
    .expect("Column 0 is not a StringArray or StringViewArray");
assert_eq!(name_col, "Alice");
}

#[tokio::test]
async fn test_chained_decoupled_sql() {
    let registry = Arc::new(NodeRegistry::default());
    let cache_dir = "target/lakehouse_test_chained";
    let engine = SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());

    let mut workflow = Workflow::new("Chained Decoupled SQL Test");

    // Node A: Source
    let node_a_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_a_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT 100 as val" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let table_a = format!("node_{}", &node_a_id.to_string()[..8]);

    // Node B: Intermediate (Queries A)
    let node_b_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_b_id,
        type_id: "core.sql".into(),
        config: json!({ "query": format!("SELECT val + 50 as val FROM {}", table_a) }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let table_b = format!("node_{}", &node_b_id.to_string()[..8]);

    // Node C: Final (Queries B)
    let node_c_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_c_id,
        type_id: "core.sql".into(),
        config: json!({ "query": format!("SELECT val * 2 as result FROM {}", table_b) }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let table_c = format!("node_{}", &node_c_id.to_string()[..8]);

    // Execute
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    // Verify
    let results = engine.ctx.sql(&format!("SELECT * FROM {}", table_c)).await.unwrap().collect().await.unwrap();
    assert_eq!(results[0].num_rows(), 1);
    let val_col = results[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().unwrap();
    assert_eq!(val_col.value(0), 300); // (100 + 50) * 2
}

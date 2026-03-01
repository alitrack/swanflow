use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{NodeInstance, Workflow, EdgeInstance};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::{NodeEvent, NodeState};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_decoupled_sql_querying() {
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    // 1. Build a workflow with two disconnected paths
    // Path A: SQL (Source) -> Materialize (Table: node_abc)
    // Path B: SQL (Query node_abc) -> Materialize
    let mut workflow = Workflow::new("Decoupled SQL Test");

    let node_a_id = Uuid::new_v4();
    let node_a_idx = workflow.add_node(NodeInstance {
        id: node_a_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT 'Alice' as name, 30 as age UNION ALL SELECT 'Bob', 25" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // Add a terminal output for A to force execution
    let node_out_a = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.output".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });
    workflow.add_edge(node_a_idx, node_out_a, EdgeInstance { source_port: 0, target_port: 0 }).unwrap();

    let table_name = format!("node_{}", &node_a_id.to_string()[..8]);

    let node_b_id = Uuid::new_v4();
    let node_b_idx = workflow.add_node(NodeInstance {
        id: node_b_id,
        type_id: "core.sql".into(),
        // Query the table produced by Node A, even though there's NO EDGE
        config: json!({ "query": format!("SELECT * FROM {} WHERE age > 28", table_name) }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // We do NOT add an edge between A and B
    // workflow.add_edge(...) - SKIPPED

    // 2. Execute Workflow
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Error { node_id, message } = event {
            panic!("Workflow failed at node {}: {}", node_id, message);
        }
    }

    // Wait a bit more for final registrations
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // 3. Verification: Check if Node B actually got the data from Node A
    // Before checking B, let's verify A is in the catalog
    let results_a = engine.ctx.sql(&format!("SELECT * FROM {}", table_name)).await.expect("Node A table not found in catalog").collect().await.unwrap();
    assert_eq!(results_a[0].num_rows(), 2);

    let table_b_name = format!("node_{}", &node_b_id.to_string()[..8]);
    let results_b = engine.ctx.sql(&format!("SELECT * FROM {}", table_b_name)).await.unwrap().collect().await.unwrap();
    
    assert_eq!(results_b.len(), 1);
    assert_eq!(results_b[0].num_rows(), 1);
    // Alice is > 28
    let name_col = results_b[0].column(0).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
    assert_eq!(name_col.value(0), "Alice");
}

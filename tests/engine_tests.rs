use std::sync::Arc;
use tokio::sync::mpsc;
use rustflow::core::graph::{Workflow, NodeInstance, EdgeInstance};
use rustflow::core::engine::SwanFlowEngine;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::sql::SqlNode;
use rustflow::nodes::io::CsvReadNode;
use rustflow::nodes::{NodeState, NodeEvent};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn test_engine_execution_with_events() {
    // 1. Create a dummy CSV file for testing
    let csv_content = "name,age
alice,30
bob,25
charlie,35";
    let csv_path = "test_data.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    // 2. Setup Registry
    let mut registry = NodeRegistry::new();
    registry.register_node(Arc::new(CsvReadNode));
    registry.register_node(Arc::new(SqlNode));
    let registry = Arc::new(registry);

    // 3. Build Workflow DAG: Read CSV -> SQL (Filter)
    let mut workflow = Workflow::new("Engine Demo");
    
    let read_node_id = Uuid::new_v4();
    let read_idx = workflow.add_node(NodeInstance {
        id: read_node_id,
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        sub_workflow: None,
    });

    let sql_node_id = Uuid::new_v4();
    let sql_idx = workflow.add_node(NodeInstance {
        id: sql_node_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT * FROM input_0 WHERE age > 28" }),
        state: NodeState::Configured,
        sub_workflow: None,
    });

    workflow.add_edge(read_idx, sql_idx, EdgeInstance { source_port: 0, target_port: 0 }).unwrap();

    // 4. Execute Workflow
    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    // 5. Verify Events
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        println!("Received Event: {:?}", event);
        events.push(event);
    }

    // Cleanup
    let _ = std::fs::remove_file(csv_path);

    // Check if we got Running and Executed events for both nodes
    let running_read = events.iter().any(|e| matches!(e, NodeEvent::StateChanged { node_id, state: NodeState::Running } if *node_id == read_node_id));
    let executed_read = events.iter().any(|e| matches!(e, NodeEvent::StateChanged { node_id, state: NodeState::Executed } if *node_id == read_node_id));
    let running_sql = events.iter().any(|e| matches!(e, NodeEvent::StateChanged { node_id, state: NodeState::Running } if *node_id == sql_node_id));
    let executed_sql = events.iter().any(|e| matches!(e, NodeEvent::StateChanged { node_id, state: NodeState::Executed } if *node_id == sql_node_id));

    assert!(running_read, "Read node should have started running");
    assert!(executed_read, "Read node should have finished execution");
    assert!(running_sql, "SQL node should have started running");
    assert!(executed_sql, "SQL node should have finished execution");
}

use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use rustflow::nodes::io::CsvReadNode;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::sql::SqlNode;
use rustflow::nodes::transform::FilterNode;
use rustflow::nodes::{NodeEvent, NodeState};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

#[tokio::test]
async fn test_engine_execution_with_events() {
    let csv_content = "name,age\nalice,30\nbob,25\ncharlie,35";
    let csv_path = "test_data_events.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let registry = Arc::new(NodeRegistry::default());
    let mut workflow = Workflow::new("Engine Events Demo");

    let read_node_id = Uuid::new_v4();
    let read_idx = workflow.add_node(NodeInstance {
        id: read_node_id,
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let sql_node_id = Uuid::new_v4();
    let sql_idx = workflow.add_node(NodeInstance {
        id: sql_node_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT * FROM input_0 WHERE age > 28" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            read_idx,
            sql_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }

    let _ = std::fs::remove_file(csv_path);

    assert!(events.iter().any(|e| matches!(e, NodeEvent::StateChanged { node_id, state: NodeState::Running } if *node_id == read_node_id)));
    assert!(events.iter().any(|e| matches!(e, NodeEvent::StateChanged { node_id, state: NodeState::Executed } if *node_id == sql_node_id)));
}

#[tokio::test]
async fn test_materialization_and_cache() {
    let csv_content = "name,val\nA,10\nB,20";
    let csv_path = "test_data_cache.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    let mut workflow = Workflow::new("Cache Test");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: true, // Enable materialization
        sub_workflow: None,
    });

    // 1. First execution: should materialize
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(_) = rx.recv().await {}

    // Verify cache directory and file exist
    let cache_dir = std::path::Path::new("target/swanflow_cache");
    assert!(cache_dir.exists());

    // 2. Second execution: should use cache (short-circuit)
    // We can delete the source CSV to prove it's using the cache
    std::fs::remove_file(csv_path).unwrap();

    let mut rx2 = engine.execute_workflow(&workflow).await.unwrap();
    let mut events = Vec::new();
    while let Some(event) = rx2.recv().await {
        events.push(event);
    }

    assert!(events.iter().any(|e| matches!(
        e,
        NodeEvent::StateChanged {
            node_id: _,
            state: NodeState::Executed
        }
    )));

    // Cleanup cache
    std::fs::remove_dir_all(cache_dir).ok();
}

#[tokio::test]
async fn test_schema_inference() {
    let csv_content = "name,age,city\nalice,30,NY";
    let csv_path = "test_data_schema.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    let mut workflow = Workflow::new("Schema Inference Test");
    let read_id = Uuid::new_v4();
    let read_idx = workflow.add_node(NodeInstance {
        id: read_id,
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let sql_id = Uuid::new_v4();
    let sql_idx = workflow.add_node(NodeInstance {
        id: sql_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT name, age FROM input_0" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            read_idx,
            sql_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let schemas = engine.infer_schemas(&workflow).await.unwrap();

    let read_schema = &schemas[&read_id][0];
    assert_eq!(read_schema.fields().len(), 3);

    let sql_schema = &schemas[&sql_id][0];
    assert_eq!(sql_schema.fields().len(), 2);
    assert!(sql_schema.field_with_name(None, "name").is_ok());
    assert!(sql_schema.field_with_name(None, "age").is_ok());
    assert!(sql_schema.field_with_name(None, "city").is_err());

    let _ = std::fs::remove_file(csv_path);
}

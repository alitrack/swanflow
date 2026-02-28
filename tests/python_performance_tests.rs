use std::sync::Arc;
use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{Workflow, NodeInstance};
use rustflow::nodes::registry::NodeRegistry;
use serde_json::json;

#[tokio::test]
async fn test_python_zero_copy_execution() {
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry.clone());
    
    let mut workflow = Workflow::new("Python Test");
    
    // 1. Create a dummy input node
    let csv_content = "val\n1\n2\n3\n4\n5";
    std::fs::write("test_python.csv", csv_content).unwrap();
    
    let reader_idx = workflow.add_node(NodeInstance {
        id: uuid::Uuid::new_v4(),
        type_id: "core.io.csv".to_string(),
        config: json!({ "path": "test_python.csv" }),
        state: rustflow::nodes::NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });
    
    // 2. Add Python Node that increments values
    let python_idx = workflow.add_node(NodeInstance {
        id: uuid::Uuid::new_v4(),
        type_id: "core.script.python".to_string(),
        config: json!({
            "script": "import pyarrow as pa\nimport pyarrow.compute as pc\n# batch is an arro3 object, convert to pyarrow\npa_batch = pa.record_batch(batch)\nval_col = pa_batch.column('val')\nnew_val = pc.add(val_col, 10)\nreturn pa.RecordBatch.from_arrays([new_val], names=['val'])"
        }),
        state: rustflow::nodes::NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });
    
    workflow.add_edge(reader_idx, python_idx, rustflow::core::graph::EdgeInstance {
        source_port: 0,
        target_port: 0,
    }).unwrap();
    
    // 3. Run and verify
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    let mut success = false;
    while let Some(event) = rx.recv().await {
        println!("  Event: {:?}", event);
        if let rustflow::nodes::NodeEvent::StateChanged { node_id, state } = event {
            if node_id == workflow.graph[python_idx].id && matches!(state, rustflow::nodes::NodeState::Executed) {
                success = true;
            }
            if let rustflow::nodes::NodeState::Error(e) = state {
                panic!("Workflow error: {}", e);
            }
        }
    }
    
    assert!(success, "Python node should have executed successfully");
    std::fs::remove_file("test_python.csv").ok();
}

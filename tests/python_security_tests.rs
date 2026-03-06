use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::{NodeEvent, NodeState};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

fn python_has_pyarrow() -> bool {
    std::process::Command::new("python3")
        .args(["-c", "import pyarrow"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn test_python_restricted_import() {
    if !python_has_pyarrow() {
        println!("Skipping python security test: python3 module 'pyarrow' is not installed.");
        return;
    }

    let registry = Arc::new(NodeRegistry::default());
    let mut workflow = Workflow::new("Python Security Test");

    let csv_content = "col1
1";
    let csv_path = "test_security.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let read_idx = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    // This script tries to import 'os' which should be blocked
    let python_idx = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.script.python".into(),
        config: json!({ "script": "import os
return batch" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            read_idx,
            python_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    let mut found_error = false;
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Error { message, .. } = event {
            if message.contains("ImportError") && message.contains("restricted") {
                found_error = true;
            }
        }
    }

    let _ = std::fs::remove_file(csv_path);
    assert!(found_error, "Should have caught a restricted ImportError");
}

#[tokio::test]
async fn test_python_valid_transform() {
    if !python_has_pyarrow() {
        println!("Skipping python security test: python3 module 'pyarrow' is not installed.");
        return;
    }

    let registry = Arc::new(NodeRegistry::default());
    let mut workflow = Workflow::new("Python Success Test");

    let csv_content = "val
10";
    let csv_path = "test_python_success.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let read_idx = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    // Valid script: multiplies column 'val' by 2
    let python_idx = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.script.python".into(),
        config: json!({ "script": "import pyarrow.compute as pc\n# batch is an arro3 RecordBatch, convert to pyarrow\npa_batch = pa.record_batch(batch)\nval = pa_batch.column('val')\nnew_val = pc.multiply(val, 2)\nnew_batch = pa.RecordBatch.from_arrays([new_val], names=['val'])\nreturn new_batch" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            read_idx,
            python_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    let mut executed = false;
    while let Some(event) = rx.recv().await {
        if let NodeEvent::StateChanged {
            state: NodeState::Executed,
            ..
        } = event
        {
            executed = true;
        }
        if let NodeEvent::Error { message, .. } = event {
            panic!("Unexpected error: {}", message);
        }
    }

    let _ = std::fs::remove_file(csv_path);
    assert!(executed, "Python script should execute successfully");
}

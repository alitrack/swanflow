use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::{NodeEvent, NodeState};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

fn should_run_engine_integration_tests() -> bool {
    std::env::var("RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        == Some("1")
}

#[tokio::test]
async fn test_multi_terminal_execution() {
    if !should_run_engine_integration_tests() {
        println!(
            "Skipping engine integration test. Set RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

    let csv_content = "name,age
alice,30
bob,25";
    let csv_path = "test_multi_term.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let registry = Arc::new(NodeRegistry::default());
    let mut workflow = Workflow::new("Multi Terminal Test");

    let read_idx = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let terminal_1_id = Uuid::new_v4();
    let terminal_1_idx = workflow.add_node(NodeInstance {
        id: terminal_1_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT name FROM input_0" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let terminal_2_id = Uuid::new_v4();
    let terminal_2_idx = workflow.add_node(NodeInstance {
        id: terminal_2_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT age FROM input_0" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            read_idx,
            terminal_1_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            read_idx,
            terminal_2_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    let mut executed_nodes = std::collections::HashSet::new();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::StateChanged {
            node_id,
            state: NodeState::Executed,
        } = event
        {
            executed_nodes.insert(node_id);
        }
    }

    let _ = std::fs::remove_file(csv_path);

    assert!(
        executed_nodes.contains(&terminal_1_id),
        "Terminal 1 should be executed"
    );
    assert!(
        executed_nodes.contains(&terminal_2_id),
        "Terminal 2 should be executed"
    );
}

#[tokio::test]
async fn test_variable_dependency_ordering() {
    if !should_run_engine_integration_tests() {
        println!(
            "Skipping engine integration test. Set RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

    let registry = Arc::new(NodeRegistry::default());
    let mut workflow = Workflow::new("Variable Dependency Test");

    let creator_id = Uuid::new_v4();
    let _creator_idx = workflow.add_node(NodeInstance {
        id: creator_id,
        type_id: "core.flow.variable_creator".into(),
        config: json!({ "variables": { "threshold": 28 } }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });
    workflow.context.insert("threshold".to_string(), json!(28));

    let csv_content = "name,age
alice,30
bob,25";
    let csv_path = "test_var_dep.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    let read_idx = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
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
        config: json!({ "query": "SELECT * FROM input_0 WHERE age > ${context.threshold}" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    // Explicit edge for data
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
    // NO explicit edge from creator to sql - compiler should handle it

    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    let mut execution_order = Vec::new();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::StateChanged {
            node_id,
            state: NodeState::Executed,
        } = event
        {
            execution_order.push(node_id);
        }
    }

    let _ = std::fs::remove_file(csv_path);

    let sql_pos = execution_order
        .iter()
        .position(|&id| id == sql_id)
        .expect("SQL should be executed");

    if let Some(creator_pos) = execution_order.iter().position(|&id| id == creator_id) {
        assert!(
            creator_pos < sql_pos,
            "Variable creator MUST execute before the consumer node"
        );
    }
}

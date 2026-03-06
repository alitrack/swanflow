use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_complex_metanode_optimization() {
    // 1. Create test data (CSV)
    let csv_content = "id,name,age,city
1,alice,30,New York
2,bob,25,Los Angeles
3,charlie,35,New York
4,david,40,Chicago";
    let csv_path = "complex_test_data.csv";
    std::fs::write(csv_path, csv_content).unwrap();

    // 2. Setup Registry
    let registry = Arc::new(NodeRegistry::default());
    if registry.get_node("core.metanode").is_err() {
        println!("Skipping metanode execution test: core.metanode is not registered.");
        let _ = std::fs::remove_file(csv_path);
        return;
    }

    // 3. Build Sub-Workflow (MetaNode): Filter by City
    let mut sub_workflow = Workflow::new("CityFilterSub");
    let sub_in = sub_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.input".into(),
        config: json!({ "port_index": 0 }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let sub_sql = sub_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT * FROM input_0 WHERE city = 'New York'" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let sub_out = sub_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.output".into(),
        config: json!({ "port_index": 0 }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    sub_workflow
        .add_edge(
            sub_in,
            sub_sql,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    sub_workflow
        .add_edge(
            sub_sql,
            sub_out,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // 4. Build Main Workflow: Read CSV -> MetaNode -> Project (Name only)
    let mut main_workflow = Workflow::new("ComplexMain");
    let read_idx = main_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let meta_idx = main_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.metanode".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: Some(Box::new(sub_workflow)),
    });
    let project_idx = main_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.transform.project".into(),
        config: json!({ "exprs": ["name", "city"] }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    main_workflow
        .add_edge(
            read_idx,
            meta_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    main_workflow
        .add_edge(
            meta_idx,
            project_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // 5. Execute
    let engine = SwanFlowEngine::new(registry);
    let mut rx = engine.execute_workflow(&main_workflow).await.unwrap();

    let mut event_count = 0;
    while let Some(event) = rx.recv().await {
        println!("Complex Workflow Event: {:?}", event);
        event_count += 1;
    }

    // Cleanup
    let _ = std::fs::remove_file(csv_path);

    assert!(
        event_count >= 6,
        "Should receive at least Running/Executed events for all nodes"
    );
}

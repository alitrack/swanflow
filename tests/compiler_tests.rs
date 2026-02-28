use datafusion::prelude::*;
use rustflow::core::compiler::WorkflowCompiler;
use rustflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::sql::SqlNode;
use rustflow::nodes::{NodeBehavior, NodeState};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_topological_sort() {
    let mut workflow = Workflow::new("Sort Test");
    let n1 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test".into(),
        config: json!({}),
        state: NodeState::Configured,
        sub_workflow: None,
    });
    let n2 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test".into(),
        config: json!({}),
        state: NodeState::Configured,
        sub_workflow: None,
    });
    let n3 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test".into(),
        config: json!({}),
        state: NodeState::Configured,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            n1,
            n2,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            n2,
            n3,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let compiler = WorkflowCompiler::new(false);
    let order = compiler.get_execution_order(&workflow).unwrap();

    assert_eq!(order.len(), 3);
}

#[tokio::test]
async fn test_metanode_expansion() {
    let mut registry = NodeRegistry::new();
    registry.register_node(Arc::new(SqlNode));

    // 1. Create Sub-Workflow (MetaNode content)
    // core.input -> core.sql (Identity)
    let mut sub_workflow = Workflow::new("SubFlow");
    let sub_input_id = Uuid::new_v4();
    let si = sub_workflow.add_node(NodeInstance {
        id: sub_input_id,
        type_id: "core.input".into(),
        config: json!({}),
        state: NodeState::Configured,
        sub_workflow: None,
    });
    let sub_sql_id = Uuid::new_v4();
    let ss = sub_workflow.add_node(NodeInstance {
        id: sub_sql_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT * FROM input_0" }),
        state: NodeState::Configured,
        sub_workflow: None,
    });
    sub_workflow
        .add_edge(
            si,
            ss,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // 2. Create Main Workflow
    // core.sql (Source) -> MetaNode (SubFlow)
    let mut main_workflow = Workflow::new("MainFlow");
    let source_id = Uuid::new_v4();
    let source_idx = main_workflow.add_node(NodeInstance {
        id: source_id,
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT 1 as val" }),
        state: NodeState::Configured,
        sub_workflow: None,
    });
    let meta_id = Uuid::new_v4();
    let meta_idx = main_workflow.add_node(NodeInstance {
        id: meta_id,
        type_id: "core.metanode".into(),
        config: json!({}),
        state: NodeState::Configured,
        sub_workflow: Some(Box::new(sub_workflow)),
    });
    main_workflow
        .add_edge(
            source_idx,
            meta_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // 3. Compile
    let mut compiler = WorkflowCompiler::new(false);
    let outputs = compiler
        .compile(&main_workflow, &registry, None)
        .await
        .unwrap();

    assert!(outputs.contains_key(&0));
    // The output should be the result of the SQL inside the MetaNode
    let plan = outputs.get(&0).unwrap();
    println!("Compiled Plan: {:?}", plan);
}

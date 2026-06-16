use async_trait::async_trait;
use swanflow::core::compiler::WorkflowCompiler;
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::{NodeBehavior, NodeState};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

struct TestNode;
#[async_trait]
impl NodeBehavior for TestNode {
    fn spec(&self) -> swanflow::nodes::NodeSpec {
        swanflow::nodes::NodeSpec {
            type_id: "test".into(),
            display_name: "Test".into(),
            description: "Test".into(),
            input_ports: vec![],
            output_ports: vec![swanflow::nodes::PortSpec {
                id: 0,
                name: "out".into(),
                description: "out".into(),
            }],
            config_schema: json!({}),
        }
    }
    fn validate(&self, _config: &serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
    async fn propose_plan(
        &self,
        _inputs: Vec<datafusion::logical_expr::LogicalPlan>,
        _config: &serde_json::Value,
    ) -> anyhow::Result<Vec<datafusion::logical_expr::LogicalPlan>> {
        let ctx = datafusion::prelude::SessionContext::new();
        Ok(vec![ctx.sql("SELECT 1").await?.into_optimized_plan()?])
    }
}

#[tokio::test]
async fn test_topological_sort() {
    let mut workflow = Workflow::new("Sort Test");
    let mut registry = NodeRegistry::new();
    registry.register_node(Arc::new(TestNode));

    let n1 = workflow.add_node(NodeInstance::with_materialize(
        Uuid::new_v4(),
        "test",
        json!({}),
        NodeState::Configured,
    ));
    let n2 = workflow.add_node(NodeInstance::with_materialize(
        Uuid::new_v4(),
        "test",
        json!({}),
        NodeState::Configured,
    ));

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

    let mut compiler = WorkflowCompiler::new(false);
    let stages = compiler.compile(&workflow, &registry, None).await.unwrap();

    assert_eq!(stages.len(), 2);
    assert_eq!(stages[0].len(), 1);
    assert_eq!(stages[1].len(), 1);
}

#[tokio::test]
async fn test_metanode_expansion() {
    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();

    // 1. Create Sub-Workflow (MetaNode content)
    let mut sub_workflow = Workflow::new("SubFlow");
    let si = sub_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.input".into(),
        config: json!({ "port_index": 0 }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let ss = sub_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT * FROM input_0" }),
        state: NodeState::Configured,
        materialize: true,
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
    let mut main_workflow = Workflow::new("MainFlow");
    let source_idx = main_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT 1 as val" }),
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
    let stages = compiler
        .compile(&main_workflow, &registry, None)
        .await
        .unwrap();

    // Stage 0: source_idx
    // Stage 1: meta_idx (expanded)
    assert!(stages.len() >= 2);
    println!("Compiled Stages: {:?}", stages.len());
}

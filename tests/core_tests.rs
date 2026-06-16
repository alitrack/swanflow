use anyhow::Result;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::json;
use std::sync::Arc;
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::{NodeBehavior, NodeSpec, NodeState, PortSpec};
use uuid::Uuid;

struct DummyNode;

#[async_trait]
impl NodeBehavior for DummyNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "test.dummy".into(),
            display_name: "Dummy Node".into(),
            description: "Used for tests".into(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "in".into(),
                description: "input".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "out".into(),
                description: "output".into(),
            }],
            config_schema: json!({ "type": "object" }),
        }
    }

    fn validate(&self, _config: &serde_json::Value) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &serde_json::Value,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
    }
}

#[test]
fn test_node_registry() {
    let mut registry = NodeRegistry::new();
    let dummy = Arc::new(DummyNode);
    registry.register_node(dummy);

    let fetched = registry.get_node("test.dummy").unwrap();
    assert_eq!(fetched.spec().display_name, "Dummy Node");
}

#[test]
fn test_workflow_graph_construction() {
    let mut workflow = Workflow::new("Test Workflow");
    let n1 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test.dummy".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });
    let n2 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test.dummy".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
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

    assert_eq!(workflow.graph.node_count(), 2);
    assert_eq!(workflow.graph.edge_count(), 1);
}

#[test]
fn test_cycle_detection() {
    let mut workflow = Workflow::new("Cyclic Workflow");
    let n1 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test.dummy".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });
    let n2 = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test.dummy".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
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
    let result = workflow.add_edge(
        n2,
        n1,
        EdgeInstance {
            source_port: 0,
            target_port: 0,
        },
    );

    assert!(result.is_err());
}

#[test]
fn test_metanode_structure() {
    let mut main_workflow = Workflow::new("Main");
    let mut sub_workflow = Workflow::new("Sub");

    let _sub_node_idx = sub_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test.dummy".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let metanode_idx = main_workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.metanode".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: Some(Box::new(sub_workflow)),
        ..Default::default()
    });

    let metanode = main_workflow.get_node(metanode_idx).unwrap();
    assert!(metanode.sub_workflow.is_some());
    assert_eq!(
        metanode.sub_workflow.as_ref().unwrap().graph.node_count(),
        1
    );
}

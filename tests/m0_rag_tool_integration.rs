use rustflow::core::compiler::WorkflowCompiler;
use rustflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn test_m0_rag_with_tool_executor_pipeline_compiles() {
    let mut workflow = Workflow::new("m0-rag-tool");

    let source = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".to_string(),
        config: json!({ "query": "SELECT 'search rustflow docs' AS prompt" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let tool_executor = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.tool_executor".to_string(),
        config: json!({ "tool_name": "web.search" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let structured = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.structured_io_contract".to_string(),
        config: json!({ "required_fields": ["answer"] }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let citation = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.citation_tracker".to_string(),
        config: json!({ "max_citations": 2 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            source,
            tool_executor,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            tool_executor,
            structured,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            structured,
            citation,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let mut compiler = WorkflowCompiler::new(false);
    let registry = NodeRegistry::default();
    let stages = compiler
        .compile(&workflow, &registry, None)
        .await
        .expect("m0 rag + tool executor workflow should compile");

    assert!(stages.len() >= 2);
}

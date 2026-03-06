use rustflow::core::compiler::WorkflowCompiler;
use rustflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn test_m0_rag_basic_pipeline_compiles() {
    let mut workflow = Workflow::new("m0-rag-basic");

    let source = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".to_string(),
        config: json!({ "query": "SELECT 'what is rustflow' AS question" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let planner = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.query_planning".to_string(),
        config: json!({ "max_subqueries": 3 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let chunker = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.semantic_chunker".to_string(),
        config: json!({ "chunk_size": 64, "overlap": 16 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let writer = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.vector_index_writer".to_string(),
        config: json!({ "embedding_dim": 384 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let retriever = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.ai.vector_retriever".to_string(),
        config: json!({ "embedding_dim": 384 }),
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
        config: json!({ "max_citations": 3 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    workflow
        .add_edge(
            source,
            planner,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            planner,
            chunker,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            chunker,
            writer,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            writer,
            retriever,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            retriever,
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
        .expect("m0 rag basic workflow should compile");

    assert!(stages.len() >= 2);
}

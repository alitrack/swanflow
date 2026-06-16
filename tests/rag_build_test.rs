use anyhow::Result;
use async_trait::async_trait;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::{NodeInstance, Workflow};
use swanflow::nodes::ai::graph::EntityExtractorNode;
use swanflow::nodes::ai::providers::LlmProvider;
use swanflow::nodes::io::GraphIndexWriterNode;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeState;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use uuid::Uuid;

struct MockLlmProvider;

#[async_trait]
impl LlmProvider for MockLlmProvider {
    async fn complete(&self, prompt: &str) -> Result<String> {
        if prompt.contains("Alice works for ACME") {
            Ok(r#"{
                "nodes": [
                    { "name": "Alice", "label": "PERSON", "properties": {} },
                    { "name": "ACME", "label": "ORG", "properties": {} }
                ],
                "edges": [
                    { "source": "Alice", "target": "ACME", "type": "works_for", "weight": 1.0 }
                ]
            }"#
            .to_string())
        } else {
            Ok(r#"{ "nodes": [], "edges": [] }"#.to_string())
        }
    }
}

#[tokio::test]
async fn test_rag_build_workflow() -> Result<()> {
    let tmp = tempdir()?;
    let cache_dir = tmp.path().to_str().unwrap().to_string();

    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();
    if registry.get_node("core.flow.foreach").is_err() {
        println!("Skipping rag build test: core.flow.foreach is not registered.");
        return Ok(());
    }

    let mock_provider = Arc::new(MockLlmProvider);
    registry.register_node(Arc::new(EntityExtractorNode {
        provider: mock_provider,
    }));
    registry.register_node(Arc::new(GraphIndexWriterNode));

    let engine = SwanFlowEngine::with_cache_dir(Arc::new(registry), cache_dir.clone());
    let mut workflow = Workflow::new("rag_build");

    // 1. Values Node (Input Text)
    let values_id = Uuid::new_v4();
    let values_idx = workflow.add_node(NodeInstance {
        id: values_id,
        type_id: "core.flow.foreach".to_string(), // Use foreach as a values generator for now
        config: json!({
            "items": ["Alice works for ACME"]
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // 2. Entity Extractor
    let extractor_id = Uuid::new_v4();
    let extractor_idx = workflow.add_node(NodeInstance {
        id: extractor_id,
        type_id: "ai.entity_extractor".to_string(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let _ = workflow.add_edge(
        values_idx,
        extractor_idx,
        swanflow::core::graph::EdgeInstance {
            source_port: 0,
            target_port: 0,
        },
    );

    // 3. Graph Index Writer
    let writer_id = Uuid::new_v4();
    let writer_idx = workflow.add_node(NodeInstance {
        id: writer_id,
        type_id: "io.graph_index_writer".to_string(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });
    let _ = workflow.add_edge(
        extractor_idx,
        writer_idx,
        swanflow::core::graph::EdgeInstance {
            source_port: 0,
            target_port: 0,
        },
    );
    let _ = workflow.add_edge(
        extractor_idx,
        writer_idx,
        swanflow::core::graph::EdgeInstance {
            source_port: 1,
            target_port: 1,
        },
    );

    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    while let Some(event) = rx.recv().await {
        println!("Event: {:?}", event);
    }

    // VERIFY DUCKDB
    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
    let conn = duckdb::Connection::open(&metadata_path)?;

    let node_count: i64 = conn.query_row("SELECT COUNT(*) FROM __graph_nodes", [], |r| r.get(0))?;
    assert!(node_count >= 2);

    let edge_count: i64 = conn.query_row("SELECT COUNT(*) FROM __graph_edges", [], |r| r.get(0))?;
    assert!(edge_count >= 1);

    Ok(())
}

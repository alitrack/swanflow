use anyhow::Result;
use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn test_rag_query_workflow() -> Result<()> {
    let tmp = tempdir()?;
    let cache_dir = tmp.path().to_str().unwrap().to_string();

    // PRE-SEED DUCKDB
    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
    {
        let conn = duckdb::Connection::open(&metadata_path)?;
        conn.execute("CREATE TABLE IF NOT EXISTS __graph_nodes (id BIGINT PRIMARY KEY, label TEXT, properties TEXT)", [])?;
        conn.execute("CREATE TABLE IF NOT EXISTS __graph_edges (source_id BIGINT, target_id BIGINT, type TEXT, weight DOUBLE, PRIMARY KEY (source_id, target_id, type))", [])?;

        let alice_id = rustflow::nodes::common::hash_string_to_i64("Alice");
        let acme_id = rustflow::nodes::common::hash_string_to_i64("ACME");

        conn.execute(
            "INSERT INTO __graph_nodes VALUES (?, ?, ?)",
            duckdb::params![alice_id, "PERSON", "{}"],
        )?;
        conn.execute(
            "INSERT INTO __graph_nodes VALUES (?, ?, ?)",
            duckdb::params![acme_id, "ORG", "{}"],
        )?;
        conn.execute(
            "INSERT INTO __graph_edges VALUES (?, ?, ?, ?)",
            duckdb::params![alice_id, acme_id, "works_for", 1.0],
        )?;
    }

    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();

    let engine = SwanFlowEngine::with_cache_dir(Arc::new(registry), cache_dir.clone());
    let mut workflow = Workflow::new("rag_query");

    // 1. Query Entities (Mock output from keyword extractor)
    let entities_id = Uuid::new_v4();
    let entities_idx = workflow.add_node(NodeInstance {
        id: entities_id,
        type_id: "core.sql".to_string(),
        config: json!({
            "query": "SELECT 'Alice' as entity"
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // 2. Graph Retriever
    let retriever_id = Uuid::new_v4();
    let retriever_idx = workflow.add_node(NodeInstance {
        id: retriever_id,
        type_id: "ai.graph_retriever".to_string(),
        config: json!({ "depth": 1 }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let _ = workflow.add_edge(
        entities_idx,
        retriever_idx,
        rustflow::core::graph::EdgeInstance {
            source_port: 0,
            target_port: 0,
        },
    );

    // 3. Output Node
    let output_id = Uuid::new_v4();
    let output_idx = workflow.add_node(NodeInstance {
        id: output_id,
        type_id: "core.output".to_string(),
        config: json!({ "port_index": 0 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });
    let _ = workflow.add_edge(
        retriever_idx,
        output_idx,
        rustflow::core::graph::EdgeInstance {
            source_port: 0,
            target_port: 0,
        },
    );

    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    while let Some(event) = rx.recv().await {
        println!("Event: {:?}", event);
    }

    // Success if it ran without error
    Ok(())
}

#[tokio::test]
async fn test_research_loop_workflow() -> Result<()> {
    let tmp = tempdir()?;
    let cache_dir = tmp.path().to_str().unwrap().to_string();

    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::with_cache_dir(registry, cache_dir.clone());

    let mut workflow = Workflow::new("recursive_research");

    // 1. Initial Query
    let query_id = Uuid::new_v4();
    let query_idx = workflow.add_node(NodeInstance {
        id: query_id,
        type_id: "core.sql".to_string(),
        config: json!({"query": "SELECT 'Future of AI' as query"}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });

    // 2. Research Loop
    let loop_id = Uuid::new_v4();
    let loop_idx = workflow.add_node(NodeInstance {
        id: loop_id,
        type_id: "core.ai.research_loop".to_string(),
        config: json!({
            "max_depth": 2,
            "max_pages_per_step": 3
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    });
    let _ = workflow.add_edge(
        query_idx,
        loop_idx,
        rustflow::core::graph::EdgeInstance {
            source_port: 0,
            target_port: 0,
        },
    );

    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    let mut events_received = 0;
    while let Some(event) = rx.recv().await {
        println!("DEBUG: [Test] Got event: {:?}", event);
        events_received += 1;
    }

    assert!(
        events_received > 0,
        "Should have received workflow execution events"
    );
    Ok(())
}

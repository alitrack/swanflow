use anyhow::Result;
use async_trait::async_trait;
use datafusion::arrow::array::{StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use swanflow::core::engine::{IntelligenceHook, SwanFlowEngine};
use swanflow::core::graph::{NodeInstance, Workflow};
use swanflow::nodes::ai::graph::ResearchState;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::transform::{SemanticCleanerNode, TableSchemaValidatorNode};
use swanflow::nodes::{NodeBehavior, NodeEvent, NodeState};
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

fn should_run_mcp_integration_tests() -> bool {
    std::env::var("RUSTFLOW_RUN_MCP_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        == Some("1")
}

fn should_run_engine_integration_tests() -> bool {
    std::env::var("RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        == Some("1")
}

// --- 1. Research State Tests ---

#[test]
fn test_research_state_deduplication() {
    let mut state = ResearchState::new(2);
    let url = "https://example.com".to_string();
    let query = "rust data science".to_string();
    assert!(!state.is_visited_url(&url));
    state.visit_url(url.clone());
    assert!(state.is_visited_url(&url));
    state.visit_query(query.clone());
    assert!(state.is_visited_query(&query));
}

#[test]
fn test_research_state_depth_limit() {
    let mut state = ResearchState::new(2);
    assert!(state.should_continue());
    state.current_depth = 2;
    assert!(!state.should_continue());
}

// --- 2. Validator Tests ---

#[tokio::test]
async fn test_validator_explicit_mapping() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("person_name", DataType::Utf8, true),
        Field::new("organization", DataType::Utf8, true),
    ]));
    let df = ctx.read_batch(RecordBatch::new_empty(schema))?;
    let input_plan = df.into_optimized_plan()?;
    let validator = TableSchemaValidatorNode;
    let config = json!({"mapping": {"label": "person_name", "properties": "organization"}});
    let output_plans = validator.propose_plan(vec![input_plan], &config).await?;
    let node_schema = output_plans[0].schema();
    assert!(node_schema.field_with_name(None, "label").is_ok());
    assert!(node_schema.field_with_name(None, "id").is_ok());
    Ok(())
}

// --- 3. Semantic Cleaning Tests ---

#[tokio::test]
async fn test_semantic_cleaner_apply_rules() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "company",
        DataType::Utf8,
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["Alibaba", "Google"]))],
    )?;
    let df = ctx.read_batch(batch)?;
    let cleaner = SemanticCleanerNode;
    let config = json!({"column": "company", "rules": {"Alibaba": "Alibaba Group"}});
    let output_plans = cleaner
        .propose_plan(vec![df.into_optimized_plan()?], &config)
        .await?;
    let physical_plan = ctx.state().create_physical_plan(&output_plans[0]).await?;
    let results = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;
    let cleaned_col = results[0]
        .column_by_name("company_cleaned")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(cleaned_col.value(0), "Alibaba Group");
    Ok(())
}

// --- 4. Self-Healing Hook Tests ---

struct MockRepairHook {
    error_count: Arc<AtomicUsize>,
}
#[async_trait]
impl IntelligenceHook for MockRepairHook {
    async fn on_error(
        &self,
        _node_id: Uuid,
        _type_id: &str,
        _config: &JsonValue,
        _error: &str,
    ) -> Option<JsonValue> {
        self.error_count.fetch_add(1, Ordering::SeqCst);
        Some(json!({"query": "SELECT 1"}))
    }
    async fn on_data_quality(&self, _node_id: Uuid, _sample: &RecordBatch) -> Option<JsonValue> {
        None
    }
}

#[tokio::test]
async fn test_self_healing_error_capture() -> Result<()> {
    if !should_run_engine_integration_tests() {
        println!(
            "Skipping intelligence engine integration test. Set RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS=1 to enable."
        );
        return Ok(());
    }

    let registry = Arc::new(NodeRegistry::default());
    let mut engine = SwanFlowEngine::new(registry);
    let error_count = Arc::new(AtomicUsize::new(0));
    engine.hook = Some(Arc::new(MockRepairHook {
        error_count: error_count.clone(),
    }));
    let mut workflow = Workflow::new("bad_sql");
    workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".to_string(),
        config: json!({"query": "SELECT invalid FROM non_existent"}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    ..Default::default()
    });
    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    while rx.recv().await.is_some() {}
    assert!(error_count.load(Ordering::SeqCst) >= 1);
    Ok(())
}

// --- 5. MCP Integration Tests ---

#[tokio::test]
async fn test_mcp_node_integration() -> Result<()> {
    if !should_run_engine_integration_tests() {
        println!(
            "Skipping intelligence engine integration test. Set RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS=1 to enable."
        );
        return Ok(());
    }

    if !should_run_mcp_integration_tests() {
        println!(
            "Skipping MCP integration test. Set RUSTFLOW_RUN_MCP_INTEGRATION_TESTS=1 to enable."
        );
        return Ok(());
    }

    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);
    let mut workflow = Workflow::new("mcp_test");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.ai.mcp_invoke".to_string(),
        config: json!({
            "tool_name": "echo", "arguments": {"msg": "integration test"},
            "server_command": "python3", "server_args": ["scripts/mock_mcp_server.py"]
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    ..Default::default()
    });
    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    let mut materialized_name = None;
    let mut parquet_file = None;
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Materialized {
            table_name,
            parquet_path,
            ..
        } = event
        {
            materialized_name = Some(table_name);
            if !parquet_path.is_empty() {
                parquet_file = Some(parquet_path);
            }
        }
    }
    let table_name = materialized_name.expect("Materialized event missing");
    let mut batch = None;
    for _ in 0..10 {
        if let Ok(df) = engine.ctx.table(&table_name).await {
            if let Ok(results) = df.collect().await {
                if !results.is_empty() {
                    batch = Some(results[0].clone());
                    break;
                }
            }
        }
        if let Some(path) = &parquet_file {
            if std::path::Path::new(path).exists() {
                if let Ok(df) = engine
                    .ctx
                    .read_parquet(path, ParquetReadOptions::default())
                    .await
                {
                    if let Ok(results) = df.collect().await {
                        if !results.is_empty() {
                            batch = Some(results[0].clone());
                            break;
                        }
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let batch = batch.expect("Materialized table should contain results");
    let content_col = batch
        .column_by_name("content")
        .expect("Column 'content' not found");
    let value = if let Some(a) = content_col.as_any().downcast_ref::<StringArray>() {
        a.value(0).to_string()
    } else if let Some(a) = content_col.as_any().downcast_ref::<StringViewArray>() {
        a.value(0).to_string()
    } else {
        panic!("Unsupported type: {:?}", content_col.data_type());
    };
    assert_eq!(value, "Echo: integration test");
    Ok(())
}

use anyhow::Result;
use async_trait::async_trait;
use rustflow::core::engine::{SwanFlowEngine, IntelligenceHook};
use rustflow::core::graph::{Workflow, NodeInstance};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

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
        println!("[Test] Mock Hook captured error");
        // Simulate a fix suggestion (though engine doesn't fully retry re-compilation yet)
        Some(json!({"query": "SELECT 1"}))
    }

    async fn on_data_quality(
        &self,
        _node_id: Uuid,
        _sample: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Option<JsonValue> {
        None
    }
}

#[tokio::test]
async fn test_self_healing_error_capture() -> Result<()> {
    let registry = Arc::new(NodeRegistry::default());
    let mut engine = SwanFlowEngine::new(registry);
    
    let error_count = Arc::new(AtomicUsize::new(0));
    let hook = Arc::new(MockRepairHook { error_count: error_count.clone() });
    engine.hook = Some(hook);

    let mut workflow = Workflow::new("bad_sql");
    workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".to_string(),
        config: json!({"query": "SELECT invalid FROM non_existent"}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let rx = engine.execute_workflow(&workflow).await?;
    let mut rx = rx;
    while let Some(_event) = rx.recv().await {
        // Wait for finish
    }

    // The hook should have been called
    assert!(error_count.load(Ordering::SeqCst) >= 1);

    Ok(())
}

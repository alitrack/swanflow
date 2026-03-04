use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use rustflow::core::compiler::WorkflowCompiler;
use rustflow::core::engine::{IntelligenceHook, SwanFlowEngine};
use rustflow::core::graph::{NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::{NodeBehavior, NodeEvent, NodeSpec, NodeState, NodeErrorClass, PortSpec};
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

struct StrictValidationNode;

#[async_trait]
impl NodeBehavior for StrictValidationNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "test.strict_validation".to_string(),
            display_name: "Strict Validation".to_string(),
            description: "Requires key=required".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "out".to_string(),
                description: "out".to_string(),
            }],
            config_schema: json!({
                "type": "object",
                "properties": {
                    "required": { "type": "string" }
                },
                "required": ["required"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["required"]
            .as_str()
            .ok_or_else(|| anyhow!("required is missing"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let ctx = datafusion::prelude::SessionContext::new();
        Ok(vec![ctx.sql("SELECT 1 as v").await?.into_optimized_plan()?])
    }
}

struct MigratingNode {
    seen_configs: Arc<Mutex<Vec<JsonValue>>>,
}

#[async_trait]
impl NodeBehavior for MigratingNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "test.migrating".to_string(),
            display_name: "Migrating".to_string(),
            description: "Migrates config from v1->v3".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "out".to_string(),
                description: "out".to_string(),
            }],
            config_schema: json!({"type": "object"}),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["file_path"]
            .as_str()
            .ok_or_else(|| anyhow!("file_path is required after migration"))?;
        Ok(())
    }

    fn config_version(&self) -> u32 {
        3
    }

    fn migrate_config(&self, mut config: JsonValue, from_version: u32, to_version: u32) -> Result<JsonValue> {
        if to_version != from_version + 1 {
            return Err(anyhow!("migration must be incremental"));
        }

        let obj = config
            .as_object_mut()
            .ok_or_else(|| anyhow!("config should be object"))?;

        match (from_version, to_version) {
            (1, 2) => {
                if let Some(path) = obj.remove("path") {
                    obj.insert("file_path".to_string(), path);
                }
            }
            (2, 3) => {
                obj.insert("normalize".to_string(), json!(true));
            }
            _ => {
                return Err(anyhow!("unsupported migration step {from_version}->{to_version}"));
            }
        }

        Ok(config)
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        self.seen_configs.lock().unwrap().push(config.clone());
        let ctx = datafusion::prelude::SessionContext::new();
        Ok(vec![ctx.sql("SELECT 1 as v").await?.into_optimized_plan()?])
    }
}

struct CountingHook {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl IntelligenceHook for CountingHook {
    async fn on_error(
        &self,
        _node_id: Uuid,
        _type_id: &str,
        _config: &JsonValue,
        _error: &str,
    ) -> Option<JsonValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        None
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
async fn test_preflight_validation_error_is_classified() {
    let mut registry = NodeRegistry::new();
    registry.register_node(Arc::new(StrictValidationNode));

    let mut workflow = Workflow::new("preflight validation");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "test.strict_validation".to_string(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let mut compiler = WorkflowCompiler::new(false);
    let err = compiler.compile(&workflow, &registry, None).await.expect_err("compile should fail");
    let message = err.to_string();

    assert!(message.contains("validation"), "unexpected error: {message}");
    assert!(message.contains(&node_id.to_string()), "unexpected error: {message}");
}

#[tokio::test]
async fn test_preflight_runs_multi_step_migration_with_defaults() {
    let seen_configs = Arc::new(Mutex::new(Vec::new()));
    let mut registry = NodeRegistry::new();
    registry.register_node(Arc::new(MigratingNode {
        seen_configs: seen_configs.clone(),
    }));

    let mut workflow = Workflow::new("migration preflight");
    workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "test.migrating".to_string(),
        config: json!({"path": "demo.csv"}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let mut compiler = WorkflowCompiler::new(false);
    compiler
        .compile(&workflow, &registry, None)
        .await
        .expect("compile should succeed after migration");

    let captured = seen_configs.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0]["file_path"], "demo.csv");
    assert_eq!(captured[0]["config_version"], 3);
    assert_eq!(captured[0]["impl_version"], 1);
}

#[tokio::test]
async fn test_unknown_node_degraded_loading_and_execution_failure() {
    let registry = Arc::new(NodeRegistry::new());
    let engine = SwanFlowEngine::new(registry);

    let mut workflow = Workflow::new("unknown node");
    let missing_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: missing_id,
        type_id: "community.unknown.node".to_string(),
        config: json!({"legacy": true}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let metadata = engine
        .infer_metadata(&workflow)
        .await
        .expect("inspect/infer should not fail for unknown node");
    let diagnostics = metadata
        .diagnostics
        .get(&missing_id)
        .cloned()
        .unwrap_or_default();
    assert!(diagnostics.iter().any(|d| d.contains("community.unknown.node")));

    let mut rx = engine
        .execute_workflow(&workflow)
        .await
        .expect("execution should return event stream even when node is missing");

    let mut saw_missing_error = false;
    while let Ok(Some(event)) = timeout(Duration::from_secs(10), rx.recv()).await {
        if let NodeEvent::Error { node_id, message } = event {
            if node_id == missing_id
                && message.contains("missing node implementation")
                && message.contains("install extension")
            {
                saw_missing_error = true;
            }
        }
    }

    assert!(saw_missing_error, "expected missing-node remediation error event");
}

#[tokio::test]
async fn test_self_healing_is_skipped_for_validation_or_migration_failures() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();

    let mut engine = SwanFlowEngine::new(Arc::new(registry));
    engine.hook = Some(Arc::new(CountingHook { calls: calls.clone() }));

    let mut workflow = Workflow::new("invalid sql config");
    workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".to_string(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let mut rx = engine.execute_workflow(&workflow).await.expect("should return event stream");
    while let Ok(Some(_)) = timeout(Duration::from_secs(10), rx.recv()).await {}

    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_self_healing_runs_for_recoverable_planning_or_execution_errors() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();

    let mut engine = SwanFlowEngine::new(Arc::new(registry));
    engine.hook = Some(Arc::new(CountingHook { calls: calls.clone() }));

    let mut workflow = Workflow::new("recoverable runtime error");
    workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".to_string(),
        config: json!({ "query": "SELECT * FROM not_existing_table" }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    let mut rx = engine.execute_workflow(&workflow).await.expect("execution stream should be created");
    while let Ok(Some(_)) = timeout(Duration::from_secs(10), rx.recv()).await {}

    assert!(calls.load(Ordering::SeqCst) >= 1);
}

#[test]
fn test_error_class_recoverability_contract() {
    assert!(!NodeErrorClass::Validation.is_recoverable());
    assert!(!NodeErrorClass::Migration.is_recoverable());
    assert!(NodeErrorClass::Planning.is_recoverable());
    assert!(NodeErrorClass::Execution.is_recoverable());
}

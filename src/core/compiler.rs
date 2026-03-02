use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use datafusion::prelude::*;
use datafusion::logical_expr::{LogicalPlan, Extension};
use datafusion::error::Result;
use datafusion::execution::context::SessionStateBuilder;
use uuid::Uuid;
use serde_json::Value as JsonValue;

use crate::core::graph::Workflow;
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeEvent;
use crate::core::observer::{StateProbeLogical, MaterializerLogical};

pub struct WorkflowCompiler {
    pub compiled_outputs: HashMap<Uuid, HashMap<usize, LogicalPlan>>,
    pub inferred_schemas: HashMap<Uuid, Vec<datafusion::common::DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<datafusion::arrow::record_batch::RecordBatch>>,
    pub node_hashes: HashMap<Uuid, String>,
    pub inject_probes: bool,
    pub dynamic_context: HashMap<String, JsonValue>,
    config_cache: HashMap<Uuid, String>,
    pub event_tx: Option<mpsc::UnboundedSender<NodeEvent>>,
    pub cache_dir: String,
    pub ctx: Arc<SessionContext>,
}

impl WorkflowCompiler {
    pub fn new(inject_probes: bool) -> Self {
        Self::with_cache_dir(inject_probes, "target/swanflow_cache".to_string())
    }

    pub fn with_cache_dir(inject_probes: bool, cache_dir: String) -> Self {
        let ctx = Arc::new(SessionContext::new());
        Self {
            compiled_outputs: HashMap::new(),
            inferred_schemas: HashMap::new(),
            sample_data: HashMap::new(),
            node_hashes: HashMap::new(),
            inject_probes,
            dynamic_context: HashMap::new(),
            config_cache: HashMap::new(),
            event_tx: None,
            cache_dir,
            ctx,
        }
    }

    pub async fn compile(
        &mut self,
        workflow: &Workflow,
        registry: &NodeRegistry,
        ctx_override: Option<Arc<SessionContext>>,
    ) -> Result<Vec<(usize, LogicalPlan)>> {
        let _ctx = ctx_override.unwrap_or_else(|| self.ctx.clone());
        
        // Simplified stub to get it compiling
        // Real implementation should use workflow.graph level access
        let terminals = Vec::new();
        Ok(terminals)
    }

    pub async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let ctx = Arc::new(SessionContext::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: tx,
            ctx: ctx.clone(),
            cache_dir: self.cache_dir.clone(),
        };
        let physical_planner = Arc::new(datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(planner)]));
        
        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(Arc::new(crate::core::engine::SwanFlowQueryPlanner { physical_planner }))
            .build();
            
        let physical_plan = state.create_physical_plan(plan).await?;
        datafusion::physical_plan::collect(physical_plan, state.task_ctx()).await
    }
}

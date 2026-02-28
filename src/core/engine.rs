use std::sync::Arc;
use std::fmt;
use std::collections::HashMap;
use tokio::sync::mpsc;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner, DefaultPhysicalPlanner};
use datafusion::execution::context::{SessionState, QueryPlanner};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::common::DFSchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use uuid::Uuid;

use crate::core::graph::Workflow;
use crate::core::compiler::WorkflowCompiler;
use crate::core::observer::{StateProbeLogical, StateProbeExec, MaterializerLogical, MaterializerExec};
use crate::core::datasource::{ExcelScanLogical, ExcelScanExec};
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeEvent;

/// Custom planner to handle StateProbe, Materializer, and Excel translation from logical to physical.
pub struct SwanFlowPlanner {
    pub event_tx: mpsc::UnboundedSender<NodeEvent>,
}

#[async_trait]
impl ExtensionPlanner for SwanFlowPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(probe) = node.as_any().downcast_ref::<StateProbeLogical>() {
            return Ok(Some(Arc::new(StateProbeExec::new(
                probe.node_id,
                physical_inputs[0].clone(),
                self.event_tx.clone(),
            ))));
        }
        if let Some(materializer) = node.as_any().downcast_ref::<MaterializerLogical>() {
            return Ok(Some(Arc::new(MaterializerExec::new(
                materializer.cache_key.clone(),
                physical_inputs[0].clone(),
            ))));
        }
        if let Some(excel) = node.as_any().downcast_ref::<ExcelScanLogical>() {
            return Ok(Some(Arc::new(ExcelScanExec::new(
                excel.path.clone(),
                excel.sheet.clone(),
                excel.mode.clone(),
                Arc::new(excel.schema.as_ref().into()),
            ))));
        }
        Ok(None)
    }
}

pub struct SwanFlowQueryPlanner {
    pub physical_planner: Arc<dyn PhysicalPlanner>,
}

impl fmt::Debug for SwanFlowQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwanFlowQueryPlanner").finish()
    }
}

#[async_trait]
impl QueryPlanner for SwanFlowQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Comprehensive metadata inferred for a workflow.
pub struct WorkflowMetadata {
    pub schemas: HashMap<Uuid, Vec<DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<RecordBatch>>,
}

pub struct SwanFlowEngine {
    pub ctx: SessionContext,
    pub registry: Arc<NodeRegistry>,
}

impl SwanFlowEngine {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        let ctx = SessionContext::new();
        Self { ctx, registry }
    }

    /// Performs schema and sample inference for all nodes in the workflow without full execution.
    pub async fn infer_metadata(&self, workflow: &Workflow) -> anyhow::Result<WorkflowMetadata> {
        let mut compiler = WorkflowCompiler::new(false);
        compiler.compile(workflow, &self.registry, None).await?;
        Ok(WorkflowMetadata {
            schemas: compiler.inferred_schemas,
            sample_data: compiler.sample_data,
        })
    }

    /// Executes a workflow DAG and returns a stream of node events.
    pub async fn execute_workflow(
        &self,
        workflow: &Workflow,
    ) -> anyhow::Result<mpsc::UnboundedReceiver<NodeEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();

        // 1. Compile Logical Graph
        let mut compiler = WorkflowCompiler::new(true);
        let terminal_outputs = compiler.compile(workflow, &self.registry, None).await?;
        
        // Pick the first terminal output for now
        let final_plan = terminal_outputs.values().next()
            .ok_or_else(|| anyhow::anyhow!("Workflow has no output"))?;

        // 2. Setup Physical Planner with our custom probe planner
        let planner = SwanFlowPlanner { event_tx: tx.clone() };
        let physical_planner = Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(planner)]));
        let query_planner = Arc::new(SwanFlowQueryPlanner { physical_planner });
        
        let state = SessionStateBuilder::new_from_existing(self.ctx.state())
            .with_query_planner(query_planner)
            .build();

        // 3. Create Physical Plan
        let physical_plan = state.create_physical_plan(final_plan).await?;

        // 4. Execute in background
        let ctx_clone = self.ctx.clone();
        tokio::spawn(async move {
            let task_ctx = ctx_clone.task_ctx();
            let result = datafusion::physical_plan::collect(physical_plan, task_ctx).await;
            if let Err(e) = result {
                eprintln!("Workflow execution error: {}", e);
            }
        });

        Ok(rx)
    }
}

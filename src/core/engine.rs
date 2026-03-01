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
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use uuid::Uuid;

use crate::core::graph::Workflow;
use crate::core::compiler::WorkflowCompiler;
use crate::core::observer::{StateProbeLogical, StateProbeExec, MaterializerLogical, MaterializerExec};
use crate::core::datasource::{ExcelScanLogical, ExcelScanExec, ExcelSinkLogical, ExcelSinkExec, ExcelTemplateSinkLogical, ExcelTemplateExec, ExcelReportSinkLogical, ExcelReportExec};
use crate::nodes::script::python::{PythonLogical, PythonExec};
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeEvent;

use duckdb::Connection;

pub struct SwanFlowEngine {
    pub ctx: SessionContext,
    pub registry: Arc<NodeRegistry>,
}

impl SwanFlowEngine {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        let cache_dir = "target/swanflow_cache";
        std::fs::create_dir_all(cache_dir).ok();
        
        let ctx = SessionContext::new();
        
        // Initialize metadata table in DuckDB directly
        let metadata_path = format!("{}/metadata.duckdb", cache_dir);
        let conn = Connection::open(&metadata_path).expect("Failed to open DuckDB metadata");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS flow_metadata (
                node_id TEXT PRIMARY KEY,
                table_name TEXT,
                parquet_path TEXT,
                schema_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        ).expect("Failed to create metadata table");

        Self { ctx, registry }
    }

    pub async fn infer_metadata(&self, workflow: &Workflow) -> anyhow::Result<WorkflowMetadata> {
        let mut compiler = WorkflowCompiler::new(false);
        compiler.compile(workflow, &self.registry, None).await?;
        
        Ok(WorkflowMetadata {
            schemas: compiler.inferred_schemas,
            sample_data: compiler.sample_data,
            profiles: HashMap::new(),
        })
    }

    pub async fn execute_workflow(
        &self,
        workflow: &Workflow,
    ) -> anyhow::Result<mpsc::UnboundedReceiver<NodeEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();

        // Register existing tables from DuckDB metadata to SessionContext
        let cache_dir = "target/swanflow_cache";
        if let Ok(conn) = Connection::open(format!("{}/metadata.duckdb", cache_dir)) {
            let mut stmt = conn.prepare("SELECT table_name, parquet_path FROM flow_metadata")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                if let Ok((table_name, path)) = row {
                    self.ctx.register_parquet(&table_name, &path, ParquetReadOptions::default()).await.ok();
                }
            }
        }

        let mut compiler = WorkflowCompiler::new(true).with_event_tx(tx.clone());
        let terminal_outputs = compiler.compile(workflow, &self.registry, None).await?;
        
        if terminal_outputs.is_empty() {
            anyhow::bail!("Workflow has no outputs");
        }

        let tx_clone = tx.clone();
        let planner = SwanFlowPlanner { event_tx: tx.clone(), ctx: self.ctx.clone() };
        let physical_planner = Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(planner)]));
        let query_planner = Arc::new(SwanFlowQueryPlanner { physical_planner });

        let ctx = self.ctx.clone();
        tokio::spawn(async move {
            for (port, logical_plan) in terminal_outputs {
                let cache_dir = "target/swanflow_cache";
                let tables_to_register = {
                    let mut tables = Vec::new();
                    if let Ok(conn) = Connection::open(format!("{}/metadata.duckdb", cache_dir)) {
                        if let Ok(mut stmt) = conn.prepare("SELECT table_name, parquet_path FROM flow_metadata") {
                            if let Ok(rows) = stmt.query_map([], |row| {
                                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                            }) {
                                for row in rows {
                                    if let Ok(res) = row {
                                        tables.push(res);
                                    }
                                }
                            }
                        }
                    }
                    tables
                };

                for (table_name, path) in tables_to_register {
                    // Register both with and without schema prefix for maximum compatibility
                    ctx.register_parquet(&table_name, &path, ParquetReadOptions::default()).await.ok();
                }

                // Small sleep to ensure all async registration background tasks are settled
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                let state = SessionStateBuilder::new_from_existing(ctx.state())
                    .with_query_planner(query_planner.clone())
                    .build();
match state.create_physical_plan(&logical_plan).await {
    Ok(physical_plan) => {
        let task_ctx = ctx.task_ctx();
        // SEQUENTIAL COLLECT: ensure one finishes before next plans
        let _ = datafusion::physical_plan::collect(physical_plan, task_ctx).await;

        // Force a catalog refresh in the session context
        // By registering a dummy table or just waiting a bit
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

                    Err(e) => {
                        let _ = tx_clone.send(NodeEvent::Error { 
                            node_id: Uuid::nil(),
                            message: format!("Planning error: {}", e)
                        });
                    }
                }
            }
        });

        Ok(rx)
    }
}

pub struct SwanFlowPlanner {
    pub event_tx: mpsc::UnboundedSender<NodeEvent>,
    pub ctx: SessionContext,
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
                materializer.node_id,
                materializer.table_name.clone(),
                materializer.cache_key.clone(),
                physical_inputs[0].clone(),
                self.ctx.clone(),
            ))));
        }
        if let Some(excel) = node.as_any().downcast_ref::<ExcelScanLogical>() {
            let schema = excel.schema.as_ref();
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(
                schema.fields().iter().map(|f| f.as_ref().clone().into()).collect::<Vec<datafusion::arrow::datatypes::Field>>()
            );
            return Ok(Some(Arc::new(ExcelScanExec::new(
                excel.path.clone(),
                excel.sheet.clone(),
                excel.mode.clone(),
                Arc::new(arrow_schema),
            ))));
        }
        if let Some(sink) = node.as_any().downcast_ref::<ExcelSinkLogical>() {
            return Ok(Some(Arc::new(ExcelSinkExec::new(
                sink.path.clone(),
                sink.sheets.clone(),
                sink.styles.clone(),
                sink.convert_to_table,
                sink.conditional_formatting.clone(),
                physical_inputs.to_vec(),
            ))));
        }
        if let Some(template_sink) = node.as_any().downcast_ref::<ExcelTemplateSinkLogical>() {
            return Ok(Some(Arc::new(ExcelTemplateExec::new(
                template_sink.template_path.clone(),
                template_sink.output_path.clone(),
                template_sink.sheet.clone(),
                template_sink.markers.clone(),
                template_sink.table_start.clone(),
                physical_inputs.to_vec(),
            ))));
        }
        if let Some(report_sink) = node.as_any().downcast_ref::<ExcelReportSinkLogical>() {
            return Ok(Some(Arc::new(ExcelReportExec::new(
                report_sink.path.clone(),
                report_sink.charts.clone(),
                physical_inputs.to_vec(),
            ))));
        }
        if let Some(python) = node.as_any().downcast_ref::<PythonLogical>() {
            return Ok(Some(Arc::new(PythonExec::new(
                python.script.clone(),
                physical_inputs[0].clone(),
            ))));
        }
        Ok(None)
    }
}

pub struct SwanFlowQueryPlanner {
    pub physical_planner: Arc<dyn PhysicalPlanner>,
}

impl std::fmt::Debug for SwanFlowQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

#[derive(Debug, Clone)]
pub struct WorkflowMetadata {
    pub schemas: HashMap<Uuid, Vec<DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<RecordBatch>>,
    pub profiles: HashMap<Uuid, RecordBatch>,
}

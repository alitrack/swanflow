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
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use uuid::Uuid;

use crate::core::graph::Workflow;
use crate::core::compiler::WorkflowCompiler;
use crate::core::observer::{StateProbeLogical, StateProbeExec, MaterializerLogical, MaterializerExec};
use crate::core::datasource::{
    ExcelScanLogical, ExcelScanExec, ExcelSinkLogical, ExcelSinkExec, 
    ExcelTemplateSinkLogical, ExcelTemplateExec, ExcelReportSinkLogical, ExcelReportExec,
    DuckDBScanLogical, DuckDBScanExec, SqlxScanLogical, SqlxScanExec,
};
use crate::nodes::sql::SqlProxyLogical;
use crate::nodes::script::python::{PythonLogical, PythonExec};
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeEvent;

use duckdb::Connection;

pub struct SwanFlowEngine {
    pub ctx: SessionContext,
    pub registry: Arc<NodeRegistry>,
    pub cache_dir: String,
}

impl SwanFlowEngine {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        Self::with_cache_dir(registry, "target/swanflow_cache".to_string())
    }

    pub fn with_cache_dir(registry: Arc<NodeRegistry>, cache_dir: String) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();
        
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

        Self { ctx, registry, cache_dir }
    }

    pub async fn infer_metadata(&self, workflow: &Workflow) -> anyhow::Result<WorkflowMetadata> {
        let mut compiler = WorkflowCompiler::with_cache_dir(false, self.cache_dir.clone());
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
        let cache_dir = self.cache_dir.clone();
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

        let mut compiler = WorkflowCompiler::with_cache_dir(true, self.cache_dir.clone()).with_event_tx(tx.clone());
        let terminal_outputs = compiler.compile(workflow, &self.registry, None).await?;
        
        if terminal_outputs.is_empty() {
            anyhow::bail!("Workflow has no outputs");
        }

        let tx_clone = tx.clone();
        let planner = SwanFlowPlanner { event_tx: tx.clone(), ctx: self.ctx.clone(), cache_dir: self.cache_dir.clone() };
        let physical_planner = Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(planner)]));
        let query_planner = Arc::new(SwanFlowQueryPlanner { physical_planner });

        let ctx = self.ctx.clone();
        let current_cache_dir = self.cache_dir.clone();
        tokio::spawn(async move {
            for (port, logical_plan) in terminal_outputs {
                let tables_to_register = {
                    let mut tables = Vec::new();
                    if let Ok(conn) = Connection::open(format!("{}/metadata.duckdb", current_cache_dir)) {
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
    pub cache_dir: String,
}

impl std::fmt::Debug for SwanFlowPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwanFlowPlanner").field("cache_dir", &self.cache_dir).finish()
    }
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
            let input = physical_inputs[0].clone();
            // Ensure we coalesce partitions before materializing to a single file
            let input = if input.properties().output_partitioning().partition_count() > 1 {
                Arc::new(datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input))
            } else {
                input
            };
            return Ok(Some(Arc::new(MaterializerExec::new(
                materializer.node_id,
                materializer.table_name.clone(),
                materializer.cache_key.clone(),
                materializer.cache_dir.clone(),
                input,
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
        if let Some(proxy) = node.as_any().downcast_ref::<SqlProxyLogical>() {
            // Late-binding: Register inputs and parse SQL now
            tracing::debug!("Planning SqlProxy for node {}, query: {}", proxy.node_id, proxy.query);
            for (i, input_plan) in proxy.inputs.iter().enumerate() {
                let table_name = format!("input_{}", i);
                let df = datafusion::prelude::DataFrame::new(self.ctx.state(), input_plan.clone());
                self.ctx.register_table(&table_name, df.into_view()).map_err(|e| {
                    datafusion::error::DataFusionError::Plan(format!("Failed to register input_{}: {}", i, e))
                })?;
            }

            let df = self.ctx.sql(&proxy.query).await.map_err(|e| {
                tracing::error!("Late-binding SQL error for node {}: {}", proxy.node_id, e);
                datafusion::error::DataFusionError::Plan(format!("Late-binding SQL error: {}", e))
            })?;
            
            let logical_plan = df.into_optimized_plan()?;
            let physical_plan = _planner.create_physical_plan(&logical_plan, _session_state).await?;
            
            // Check if schema matches logical proxy schema
            let logical_schema = proxy.schema.clone();
            let physical_schema = physical_plan.schema();
            
            if logical_schema.as_ref() != &DFSchema::try_from(physical_schema.as_ref().clone())? {
                tracing::warn!("Schema mismatch for node {}: logical={:?}, physical={:?}. Aligning...", proxy.node_id, logical_schema, physical_schema);
            }
            
            // To fix this, we return a physical plan whose schema matches proxy.schema exactly.
            // If they differ, we use a projection.
            let physical_plan = self.align_schemas(physical_plan, logical_schema.as_ref())?;
            
            return Ok(Some(physical_plan));
        }
        if let Some(python) = node.as_any().downcast_ref::<PythonLogical>() {
            return Ok(Some(Arc::new(PythonExec::new(
                python.script.clone(),
                physical_inputs[0].clone(),
            ))));
        }
        if let Some(duck_scan) = node.as_any().downcast_ref::<DuckDBScanLogical>() {
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(
                duck_scan.schema.fields().iter().map(|f| f.as_ref().clone().into()).collect::<Vec<datafusion::arrow::datatypes::Field>>()
            );
            return Ok(Some(Arc::new(DuckDBScanExec::new(
                duck_scan.connection_path.clone(),
                duck_scan.extensions.clone(),
                duck_scan.attach_sql.clone(),
                duck_scan.query.clone(),
                Arc::new(arrow_schema),
            ))));
        }
        if let Some(sqlx_scan) = node.as_any().downcast_ref::<SqlxScanLogical>() {
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(
                sqlx_scan.schema.fields().iter().map(|f| f.as_ref().clone().into()).collect::<Vec<datafusion::arrow::datatypes::Field>>()
            );
            return Ok(Some(Arc::new(SqlxScanExec::new(
                sqlx_scan.url.clone(),
                sqlx_scan.query.clone(),
                Arc::new(arrow_schema),
            ))));
        }
        Ok(None)
    }
}

impl SwanFlowPlanner {
    fn align_schemas(&self, plan: Arc<dyn ExecutionPlan>, target: &DFSchema) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_schema = plan.schema();
        
        // Check if actually different
        let plan_df_schema = DFSchema::try_from(plan_schema.as_ref().clone())?;
        if plan_df_schema == *target {
            return Ok(plan);
        }

        // Create a projection to match the target schema
        let mut exprs = Vec::new();
        for field in target.fields() {
            if let Ok(idx) = plan_schema.index_of(field.name()) {
                exprs.push((
                    datafusion::physical_expr::expressions::col(field.name(), &plan_schema)?,
                    field.name().to_string(),
                ));
            } else {
                // Column missing in physical plan, add as NULL
                let null_expr = datafusion::physical_expr::expressions::Literal::new(
                    ScalarValue::try_from(field.data_type())?
                );
                exprs.push((Arc::new(null_expr), field.name().to_string()));
            }
        }

        Ok(Arc::new(datafusion::physical_plan::projection::ProjectionExec::try_new(
            exprs,
            plan,
        )?))
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

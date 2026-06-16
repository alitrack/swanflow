use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use serde_json::json;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::core::datasource::{
    DuckDBScanExec, DuckDBScanLogical, ExcelReportExec, ExcelReportSinkLogical, ExcelScanExec,
    ExcelScanLogical, ExcelSinkExec, ExcelSinkLogical, ExcelTemplateExec, ExcelTemplateSinkLogical,
    SqlxScanExec, SqlxScanLogical,
};
use crate::core::engine::sync_metadata_to_context;
use crate::core::observer::{MaterializerExec, MaterializerLogical, StateProbeExec, StateProbeLogical};
use crate::nodes::missing::{MissingNodeExec, MissingNodeLogical};
use crate::nodes::script::python::{PythonExec, PythonLogical};
use crate::nodes::sql::SqlProxyLogical;
use crate::nodes::NodeEvent;

#[derive(Clone)]
pub struct SwanFlowPlanner {
    pub event_tx: UnboundedSender<NodeEvent>,
    pub ctx: Arc<SessionContext>,
    pub cache_dir: String,
}

impl std::fmt::Debug for SwanFlowPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwanFlowPlanner")
            .field("cache_dir", &self.cache_dir)
            .finish()
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
            return Ok(Some(Arc::new(MaterializerExec::new(
                materializer.node_id,
                materializer.table_name.clone(),
                materializer.cache_key.clone(),
                materializer.cache_dir.clone(),
                physical_inputs[0].clone(),
            ))));
        }
        if let Some(missing) = node.as_any().downcast_ref::<MissingNodeLogical>() {
            return Ok(Some(Arc::new(MissingNodeExec::new(missing))));
        }
        if let Some(entity_port) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::graph::EntityPortLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::graph::EntityExtractorExec::new(
                    physical_inputs[0].clone(),
                    entity_port.base.entity_types.clone(),
                    entity_port.base.relation_types.clone(),
                    entity_port.base.provider.clone(),
                    entity_port.port,
                ),
            )));
        }
        if let Some(_indexer) = node
            .as_any()
            .downcast_ref::<crate::nodes::io::GraphIndexWriterLogical>()
        {
            return Ok(Some(Arc::new(crate::nodes::io::GraphIndexWriterExec::new(
                physical_inputs[0].clone(),
                physical_inputs[1].clone(),
                self.cache_dir.clone(),
            ))));
        }
        if let Some(retriever) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::graph::GraphRetrieverLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::graph::GraphRetrieverExec::new(
                    physical_inputs[0].clone(),
                    retriever.depth,
                    self.cache_dir.clone(),
                ),
            )));
        }
        if let Some(mcp) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::mcp::McpInvokeLogical>()
        {
            let arguments = serde_json::from_str(&mcp.arguments).unwrap_or(serde_json::Value::Null);
            return Ok(Some(Arc::new(crate::nodes::ai::mcp::McpInvokeExec::new(
                mcp.tool_name.clone(),
                arguments,
                mcp.server_command.clone(),
                mcp.server_args.clone(),
                Arc::new(mcp.schema.as_arrow().clone()),
            ))));
        }
        if let Some(gap) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::graph::InformationGapLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::graph::InformationGapExec::new(
                    physical_inputs[0].clone(),
                    physical_inputs[1].clone(),
                    gap.provider.clone(),
                ),
            )));
        }
        if let Some(research) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::graph::ResearchLoopLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::graph::ResearchLoopExec::new(
                    physical_inputs[0].clone(),
                    research.max_depth,
                    research.max_pages,
                    research.state.clone(),
                    research.port,
                    self.cache_dir.clone(),
                ),
            )));
        }
        if let Some(boxed) = node
            .as_any()
            .downcast_ref::<crate::nodes::script::boxed_python::BoxedPythonLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::script::boxed_python::BoxedPythonExec::new(
                    physical_inputs[0].clone(),
                    boxed.code.clone(),
                    boxed.image.clone(),
                    boxed.s3_bucket.clone(),
                    Arc::new(boxed.schema.as_arrow().clone()),
                ),
            )));
        }
        if let Some(excel_scan) = node.as_any().downcast_ref::<ExcelScanLogical>() {
            let fields = excel_scan
                .schema
                .fields()
                .iter()
                .map(|f| {
                    datafusion::arrow::datatypes::Field::new(
                        f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    )
                })
                .collect::<Vec<_>>();
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);
            return Ok(Some(Arc::new(ExcelScanExec::new(
                excel_scan.path.clone(),
                excel_scan.sheet.clone(),
                excel_scan.mode.clone(),
                Arc::new(arrow_schema),
            ))));
        }
        if let Some(excel_sink) = node.as_any().downcast_ref::<ExcelSinkLogical>() {
            return Ok(Some(Arc::new(ExcelSinkExec::new(
                excel_sink.path.clone(),
                vec![excel_sink
                    .sheets
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Sheet1".to_string())],
                json!({}),
                true,
                json!({}),
                vec![physical_inputs[0].clone()],
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

            // LAZY REGISTRATION: Sync materialized tables from metadata.duckdb
            sync_metadata_to_context(&self.ctx, &self.cache_dir).await;

            for (i, input_plan) in proxy.inputs.iter().enumerate() {
                let table_name = format!("input_{}", i);
                let df = datafusion::prelude::DataFrame::new(self.ctx.state(), input_plan.clone());
                let _ = self.ctx.deregister_table(&table_name);
                self.ctx
                    .register_table(&table_name, df.into_view())
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "Failed to register input_{}: {}",
                            i, e
                        ))
                    })?;
            }

            let mut query = proxy.query.clone();

            // WORKAROUND for DataFusion 52.1.0 sql() limitation with $1 placeholders
            for (i, param) in proxy.params.iter().enumerate() {
                let placeholder = format!("${}", i + 1);
                let val_str = match param {
                    ScalarValue::Utf8(Some(s)) => format!("'{}'", s.replace("'", "''")),
                    _ => format!("{}", param),
                };
                query = query.replace(&placeholder, &val_str);
            }

            let df = self.ctx.sql(&query).await.map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("Late-binding SQL error: {}", e))
            })?;

            let logical_plan = df.into_optimized_plan()?;
            // We MUST use the latest session state from our context which now has the tables!
            let physical_plan = _planner
                .create_physical_plan(&logical_plan, &self.ctx.state())
                .await?;

            // Check if schema matches logical proxy schema
            let logical_schema = proxy.schema.clone();
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
            let fields = duck_scan
                .schema
                .fields()
                .iter()
                .map(|f| {
                    datafusion::arrow::datatypes::Field::new(
                        f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    )
                })
                .collect::<Vec<_>>();
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);
            return Ok(Some(Arc::new(DuckDBScanExec::new(
                duck_scan.connection_path.clone(),
                duck_scan.extensions.clone(),
                duck_scan.attach_sql.clone(),
                duck_scan.query.clone(),
                Arc::new(arrow_schema),
            ))));
        }
        if let Some(sqlx_scan) = node.as_any().downcast_ref::<SqlxScanLogical>() {
            let fields = sqlx_scan
                .schema
                .fields()
                .iter()
                .map(|f| {
                    datafusion::arrow::datatypes::Field::new(
                        f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                    )
                })
                .collect::<Vec<_>>();
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(fields);
            return Ok(Some(Arc::new(SqlxScanExec::new(
                sqlx_scan.url.clone(),
                sqlx_scan.query.clone(),
                Arc::new(arrow_schema),
                sqlx_scan.params.clone(),
            ))));
        }
        Ok(None)
    }
}

impl SwanFlowPlanner {
    fn align_schemas(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        target: &DFSchema,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan_schema = plan.schema();
        let plan_df_schema = DFSchema::try_from(plan_schema.as_ref().clone())?;
        if plan_df_schema == *target {
            return Ok(plan);
        }

        let mut exprs = Vec::new();
        for field in target.fields() {
            if let Ok(_idx) = plan_schema.index_of(field.name()) {
                exprs.push((
                    datafusion::physical_expr::expressions::col(field.name(), &plan_schema)?,
                    field.name().to_string(),
                ));
            } else {
                let null_expr = datafusion::physical_expr::expressions::Literal::new(
                    ScalarValue::try_from(field.data_type())?,
                );
                exprs.push((Arc::new(null_expr), field.name().to_string()));
            }
        }

        Ok(Arc::new(
            datafusion::physical_plan::projection::ProjectionExec::try_new(exprs, plan)?,
        ))
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

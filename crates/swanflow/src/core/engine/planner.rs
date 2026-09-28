use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::core::datasource::{
    DuckDBScanExec, DuckDBScanLogical, DuckDBWriteSinkExec, DuckDBWriteSinkLogical,
    ExcelReportExec, ExcelReportSinkLogical, ExcelScanExec, ExcelScanLogical, ExcelSinkExec,
    ExcelSinkLogical, ExcelTemplateExec, ExcelTemplateSinkLogical, SqlxScanExec, SqlxScanLogical,
};
use crate::core::engine::sync_metadata_to_context;
use crate::core::observer::{
    MaterializerExec, MaterializerLogical, StateProbeExec, StateProbeLogical,
};
use crate::nodes::missing::{MissingNodeExec, MissingNodeLogical};
use crate::nodes::script::python::{PythonExec, PythonLogical};
use crate::nodes::sql::SqlProxyLogical;
use crate::nodes::transform::PivotProxyLogical;
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
        if let Some(xform) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::retrieval::TextXformLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::retrieval::TextXformExec::new(
                    physical_inputs[0].clone(),
                    xform.column.clone(),
                    xform.mode,
                    xform.config.clone(),
                ),
            )));
        }
        if let Some(pii) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::privacy::PiiLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::privacy::PiiExec::new(
                    physical_inputs[0].clone(),
                    pii.column.clone(),
                    pii.mode.clone(),
                    pii.detect_config.clone(),
                    pii.mask_config.clone(),
                ),
            )));
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
                    retriever.entity_name.clone(),
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
        // core.ai.citation_tracker: real implementation since 2026-09-28
        // (was a pass-through shell): ranks citation rows by score desc,
        // tie-breaks source_id/start, truncates to max_citations.
        if let Some(cr) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::contracts::CitationRankLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::contracts::CitationRankExec::new(
                    cr.max_citations,
                    Arc::new(cr.schema.as_arrow().clone()),
                    physical_inputs[0].clone(),
                ),
            )));
        }
        // core.ai.fact_claim_verifier: real implementation since 2026-09-28
        // (was a pass-through shell; see swanflow-workflows/docs/shell_nodes_audit.md).
        // Scores claim tokens against the evidence corpus of the input batch.
        if let Some(fv) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::verification::FactVerifyLogical>()
        {
            return Ok(Some(Arc::new(crate::nodes::ai::verification::FactVerifyExec::new(
                f32::from_bits(fv.threshold_bits),
                fv.require_evidence,
                "claim".to_string(),
                "evidence".to_string(),
                Arc::new(fv.schema.as_arrow().clone()),
                physical_inputs[0].clone(),
            ))));
        }
        // core.ai.mcp_tool: the logical node was registered and planned by
        // McpToolNode::propose_plan but never lowered here — every workflow
        // using it failed physical planning with "No physical plan found for
        // user defined logical node: McpTool" (McpToolExec was unreachable
        // dead code). Caught by wf163 E2E design audit 2026-09-28.
        if let Some(mcp) = node
            .as_any()
            .downcast_ref::<crate::nodes::mcp::McpToolLogical>()
        {
            let tool_args =
                serde_json::from_str(&mcp.tool_args).unwrap_or(serde_json::Value::Null);
            return Ok(Some(Arc::new(crate::nodes::mcp::McpToolExec::new(
                mcp.server_command.clone(),
                mcp.server_args.clone(),
                mcp.tool_name.clone(),
                tool_args,
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
        if let Some(embedder) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::vector::EmbedderLogical>()
        {
            return Ok(Some(Arc::new(crate::nodes::ai::vector::EmbedderExec::new(
                physical_inputs[0].clone(),
                embedder.endpoint.clone(),
                embedder.column.clone(),
                embedder.batch_size,
            ))));
        }
        if let Some(search) = node
            .as_any()
            .downcast_ref::<crate::nodes::ai::vector::VectorSearchLogical>()
        {
            return Ok(Some(Arc::new(
                crate::nodes::ai::vector::VectorSearchExec::new(
                    physical_inputs[0].clone(),
                    search.endpoint.clone(),
                    search.query.clone(),
                    search.top_k,
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
        if let Some(duckdb_write) = node.as_any().downcast_ref::<DuckDBWriteSinkLogical>() {
            return Ok(Some(Arc::new(DuckDBWriteSinkExec::new(
                duckdb_write.db_path.clone(),
                duckdb_write.table.clone(),
                physical_inputs.to_vec(),
                physical_inputs
                    .first()
                    .map(|p| p.schema())
                    .unwrap_or_else(|| Arc::new(datafusion::arrow::datatypes::Schema::empty())),
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
                if let Err(first) = self.ctx.register_table(&table_name, df.into_view()) {
                    // Concurrent fan_out children share this context and may race on
                    // the same alias (identical parent output). Deregister + retry once.
                    let _ = self.ctx.deregister_table(&table_name);
                    let df2 =
                        datafusion::prelude::DataFrame::new(self.ctx.state(), input_plan.clone());
                    self.ctx
                        .register_table(&table_name, df2.into_view())
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "Failed to register input_{}: {} (first: {})",
                                i, e, first
                            ))
                        })?;
                }
            }

            // WORKAROUND for DataFusion 52.1.0 sql() limitation with $1 placeholders
            let query = inline_placeholders(&proxy.query, &proxy.params);

            let df = self.ctx.sql(&query).await.map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("Late-binding SQL error: {}", e))
            })?;

            let logical_plan = df.into_optimized_plan()?;
            // We MUST use the latest session state from our context which now has the tables!
            // Check if schema matches logical proxy schema
            let logical_schema = proxy.schema.clone();
            let physical_plan = _planner
                .create_physical_plan(&logical_plan, &self.ctx.state())
                .await?;


            // Check if schema matches logical proxy schema
            let logical_schema = proxy.schema.clone();
            let physical_plan = self.align_schemas(physical_plan, logical_schema.as_ref())?;
            return Ok(Some(physical_plan));
        }
        if let Some(ducksql) = node.as_any().downcast_ref::<crate::nodes::sql_duckdb::DuckSqlLogical>() {
            // physical_inputs are already the physical children of this node
            // (engine resolves LogicalPlan::inputs() before calling us).
            // DuckSqlExec::new wraps each in CoalescePartitionsExec itself.
            let exec = crate::nodes::sql_duckdb::DuckSqlExec::new(
                ducksql.query.clone(),
                physical_inputs[0].clone(),
                physical_inputs[1..].to_vec(),
                Arc::new(ducksql.schema.as_arrow().clone()),
            );
            return Ok(Some(Arc::new(exec)));
        }
        if let Some(pivot) = node.as_any().downcast_ref::<PivotProxyLogical>() {
            // The pivot's result was COMPUTED at plan time (data-dependent
            // schema: one output column per distinct column-value). We turn
            // those pre-computed rows into a self-contained MemTable scan in a
            // FRESH context — no runtime SQL, and crucially no dependency on
            // the shared mutable `input_0` table state at run time (re-running
            // the pivot SQL over the shared ctx at execution time was flaky:
            // an earlier node in the same stage had re-registered input_0).
            let arrow_schema = datafusion::arrow::datatypes::Schema::new(
                pivot
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
                    .collect::<Vec<_>>(),
            );
            let mem = datafusion::datasource::MemTable::try_new(
                Arc::new(arrow_schema.clone()),
                vec![pivot.result.clone()],
            )
            .map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Failed to build pivot MemTable: {}",
                    e
                ))
            })?;
            let fresh = datafusion::prelude::SessionContext::new();
            fresh.register_table("pivot_result", Arc::new(mem)).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Failed to register pivot_result: {}",
                    e
                ))
            })?;
            let df = fresh.sql("SELECT * FROM pivot_result").await?;
            let plan = df.into_unoptimized_plan();
            return Ok(Some(
                _planner
                    .create_physical_plan(&plan, &fresh.state())
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "Failed to plan pivot MemTable scan: {}",
                            e
                        ))
                    })?,
            ));
        }
        if let Some(python) = node.as_any().downcast_ref::<PythonLogical>() {
            // The physical plan must advertise the same schema the logical plan
            // reports (probed/declared override), or DataFusion rejects it as a
            // mismatched schema.
            if let Some(df_schema) = &python.schema_override {
                let arrow_schema = datafusion::arrow::datatypes::SchemaRef::new(
                    df_schema.as_arrow().clone(),
                );
                return Ok(Some(Arc::new(PythonExec::with_schema(
                    python.script.clone(),
                    physical_inputs[0].clone(),
                    arrow_schema,
                ))));
            }
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
            return Ok(Some(Arc::new(DuckDBScanExec::with_inputs(
                duck_scan.connection_path.clone(),
                duck_scan.extensions.clone(),
                duck_scan.attach_sql.clone(),
                duck_scan.query.clone(),
                Arc::new(arrow_schema),
                physical_inputs.to_vec(),
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

/// Inline `$N` placeholders into a late-binding SQL string (DataFusion 52
/// `ctx.sql()` cannot bind parameters directly).
///
/// Correctness rules:
/// - Replace **descending** by placeholder number ($11 before $10 before $1)
///   so `$1` never clobbers the `$1` prefix inside `$10`/`$11`.
/// - A placeholder token must be followed by a non-digit boundary, so an
///   inline `$12` in the *value* of param $1 cannot be re-matched by a
///   later, smaller placeholder.
/// - String literals are single-quote-escaped; other types use their Display
///   form (numbers, booleans, `NULL`).
fn inline_placeholders(query: &str, params: &[ScalarValue]) -> String {
    let mut out = query.to_string();
    for i in (0..params.len()).rev() {
        let placeholder = format!("${}", i + 1);
        let val_str = match &params[i] {
            ScalarValue::Utf8(Some(s)) => format!("'{}'", s.replace('\'', "''")),
            other => format!("{}", other),
        };
        out = replace_placeholder_desc(&out, &placeholder, &val_str);
    }
    out
}

/// Replace `placeholder` (e.g. `"$1"`) in `haystack` with `replacement`,
/// only where the placeholder is NOT immediately followed by a digit
/// (`$1` must not match the prefix of `$10`).
fn replace_placeholder_desc(haystack: &str, placeholder: &str, replacement: &str) -> String {
    let bytes = haystack.as_bytes();
    let mut result = String::with_capacity(haystack.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if haystack[i..].starts_with(placeholder) {
            let after = i + placeholder.len();
            let followed_by_digit = after < bytes.len()
                && (bytes[after].is_ascii_digit());
            if !followed_by_digit {
                // Prefix [0..i) was already pushed incrementally; append only
                // the replacement (re-scanning stops after it, so a value that
                // itself contains "$N" is never inlined again).
                result.push_str(replacement);
                i = after;
                continue;
            }
        }
        result.push(haystack[i..].chars().next().unwrap());
        i += 1;
    }
    result
}

#[cfg(test)]
mod inline_placeholder_tests {
    use super::*;

    #[test]
    fn prefix_collision_is_safe_with_descending_replacement() {
        let query = "SELECT * FROM t WHERE a = $1 AND b = $10 AND c = $11";
        let params = vec![
            ScalarValue::Utf8(Some("ONE".to_string())),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(4)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(6)),
            ScalarValue::Int64(Some(7)),
            ScalarValue::Int64(Some(8)),
            ScalarValue::Int64(Some(9)),
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(11)),
        ];
        let out = inline_placeholders(query, &params);
        assert_eq!(
            out,
            "SELECT * FROM t WHERE a = 'ONE' AND b = 10 AND c = 11"
        );
    }

    #[test]
    fn value_containing_placeholder_token_is_not_rematched() {
        // Param $1's value itself contains "$2" — must not be inlined again.
        let query = "SELECT $1, $2";
        let params = vec![
            ScalarValue::Utf8(Some("$2".to_string())),
            ScalarValue::Int64(Some(7)),
        ];
        let out = inline_placeholders(query, &params);
        assert_eq!(out, "SELECT '$2', 7");
    }

    #[test]
    fn single_param_string_escaping() {
        let out = inline_placeholders(
            "SELECT * FROM t WHERE name = $1",
            &[ScalarValue::Utf8(Some("O'Brien".to_string()))],
        );
        assert_eq!(out, "SELECT * FROM t WHERE name = 'O''Brien'");
    }
}

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float32Array, RecordBatch, StringBuilder, StringArray, UInt64Array, UInt64Builder, Float32Builder};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{Extension, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CitationItem {
    pub source_id: String,
    pub snippet: String,
    pub score: f32,
    pub start: usize,
    pub end: usize,
}

pub struct StructuredIoContractNode;
pub struct CitationTrackerNode;

impl CitationTrackerNode {
    pub fn rank_citations(
        &self,
        mut citations: Vec<CitationItem>,
        config: &JsonValue,
    ) -> Result<Vec<CitationItem>> {
        let max_citations = config["max_citations"].as_u64().unwrap_or(5) as usize;
        if max_citations == 0 {
            return Err(anyhow!("max_citations must be greater than zero"));
        }

        citations.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_id.cmp(&right.source_id))
                .then_with(|| left.start.cmp(&right.start))
        });
        citations.truncate(max_citations);
        Ok(citations)
    }
}

#[async_trait]
impl NodeBehavior for StructuredIoContractNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.structured_io_contract".to_string(),
            display_name: "Structured IO Contract".to_string(),
            description: "[SHELL NODE — no runtime behavior] Validate structured output contract requirements.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Validated output plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "required_fields": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "required": ["required_fields"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let required_fields = config["required_fields"]
            .as_array()
            .ok_or_else(|| anyhow!("required_fields must be an array"))?;

        if required_fields.is_empty() {
            return Err(anyhow!("required_fields must not be empty"));
        }

        for field in required_fields {
            if field.as_str().is_none() {
                return Err(anyhow!("required_fields must contain only strings"));
            }
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Structured IO Contract requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[async_trait]
impl NodeBehavior for CitationTrackerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.citation_tracker".to_string(),
            display_name: "Citation Tracker".to_string(),
            description: "Rank input citation rows (source_id/snippet/score/start/end) by score desc, tie-break source_id/start, truncate to max_citations (offline).".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Plan with citation contract".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_citations": { "type": "integer", "minimum": 1, "default": 5 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(max) = config["max_citations"].as_u64() {
            if max == 0 {
                return Err(anyhow!("max_citations must be greater than zero"));
            }
            return Ok(());
        }
        if config.get("max_citations").is_some() {
            return Err(anyhow!("max_citations must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        mut inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Citation Tracker requires one input plan"));
        }
        self.validate(config)?;
        let max_citations = config["max_citations"].as_u64().unwrap_or(5) as usize;
        let out_schema = Arc::new(DFSchema::try_from(
            datafusion::arrow::datatypes::Schema::new(vec![
                Field::new("source_id", DataType::Utf8, false),
                Field::new("snippet", DataType::Utf8, false),
                Field::new("score", DataType::Float32, false),
                Field::new("start", DataType::UInt64, false),
                Field::new("end", DataType::UInt64, false),
            ]),
        )?);
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(CitationRankLogical {
                max_citations,
                schema: out_schema,
                input: Arc::new(inputs.remove(0)),
            }),
        })])
    }
}

// --- Extension wiring (real implementation, 2026-09-28; was a pass-through shell) ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CitationRankLogical {
    pub max_citations: usize,
    pub schema: Arc<DFSchema>,
    /// The (single) upstream plan — carried so DataFusion lowers the input.
    pub input: Arc<LogicalPlan>,
}

impl PartialOrd for CitationRankLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.max_citations.partial_cmp(&other.max_citations)
    }
}

impl UserDefinedLogicalNodeCore for CitationRankLogical {
    fn name(&self) -> &str {
        "CitationRank"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CitationRank: max_citations={}", self.max_citations)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        let input = inputs
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("CitationRank requires its input".into()))?;
        Ok(Self {
            input: Arc::new(input),
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub struct CitationRankExec {
    pub max_citations: usize,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
    pub input: Arc<dyn ExecutionPlan>,
}

impl CitationRankExec {
    pub fn new(
        max_citations: usize,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { max_citations, schema, cache, input }
    }
}

impl DisplayAs for CitationRankExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CitationRankExec: max_citations={}", self.max_citations)
    }
}

impl ExecutionPlan for CitationRankExec {
    fn name(&self) -> &str {
        "CitationRankExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CitationRankExec::new(
            self.max_citations,
            self.schema.clone(),
            children.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal("CitationRankExec requires its input child".into())
            })?,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(CitationRankStream {
            schema: self.schema.clone(),
            max_citations: self.max_citations,
            input,
            done: false,
        }))
    }
}

pub struct CitationRankStream {
    schema: SchemaRef,
    max_citations: usize,
    input: SendableRecordBatchStream,
    done: bool,
}

impl Stream for CitationRankStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let out = rank_batch(&batch, self.max_citations, self.schema.clone())?;
                Poll::Ready(Some(Ok(out)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for CitationRankStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Rank one batch of citation rows: score desc, tie-break source_id asc then
/// start asc (mirrors CitationTrackerNode::rank_citations), truncate to max.
fn rank_batch(
    batch: &RecordBatch,
    max_citations: usize,
    schema: SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let col = |name: &str| {
        batch.column_by_name(name).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "CitationRank: input missing column '{name}'"
            ))
        })
    };
    let source_id = col("source_id")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("source_id must be Utf8".into()))?;
    let snippet = col("snippet")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("snippet must be Utf8".into()))?;
    // Accept Float32 (native) or Float64 (CSV sources infer f64) for score.
    let score_f64;
    let score_f32;
    let score_vals: &dyn Fn(usize) -> f64 = if let Some(a) =
        batch.column_by_name("score").and_then(|c| {
            c.as_any().downcast_ref::<Float32Array>()
        }) {
        score_f32 = a;
        &|i: usize| score_f32.value(i) as f64
    } else if let Some(a) = batch
        .column_by_name("score")
        .and_then(|c| c.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>())
    {
        score_f64 = a;
        &|i: usize| score_f64.value(i)
    } else {
        return Err(DataFusionError::Execution(
            "score must be Float32 or Float64".into(),
        ));
    };
    // Accept UInt64 (native) or Int64 (CSV inference) for start/end.
    let start_i64;
    let start_u64;
    let start_val: &dyn Fn(usize) -> u64 = if let Some(a) = batch
        .column_by_name("start")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
    {
        start_u64 = a;
        &|i: usize| start_u64.value(i)
    } else if let Some(a) = batch
        .column_by_name("start")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
        })
    {
        start_i64 = a;
        &|i: usize| start_i64.value(i) as u64
    } else {
        return Err(DataFusionError::Execution(
            "start must be UInt64 or Int64".into(),
        ));
    };
    let end_i64;
    let end_u64;
    let end_val: &dyn Fn(usize) -> u64 = if let Some(a) = batch
        .column_by_name("end")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
    {
        end_u64 = a;
        &|i: usize| end_u64.value(i)
    } else if let Some(a) = batch
        .column_by_name("end")
        .and_then(|c| {
            c.as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
        })
    {
        end_i64 = a;
        &|i: usize| end_i64.value(i) as u64
    } else {
        return Err(DataFusionError::Execution(
            "end must be UInt64 or Int64".into(),
        ));
    };

    let mut order: Vec<usize> = (0..source_id.len()).collect();
    order.sort_by(|&l, &r| {
        score_vals(r)
            .partial_cmp(&score_vals(l))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| source_id.value(l).cmp(source_id.value(r)))
            .then_with(|| start_val(l).cmp(&start_val(r)))
    });
    order.truncate(max_citations);

    let mut sid_b = StringBuilder::new();
    let mut snip_b = StringBuilder::new();
    let mut score_b = Float32Builder::new();
    let mut start_b = UInt64Builder::new();
    let mut end_b = UInt64Builder::new();
    for i in order {
        sid_b.append_value(source_id.value(i));
        snip_b.append_value(snippet.value(i));
        score_b.append_value(score_vals(i) as f32);
        start_b.append_value(start_val(i));
        end_b.append_value(end_val(i));
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(sid_b.finish()),
            Arc::new(snip_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(start_b.finish()),
            Arc::new(end_b.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod citation_rank_tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    fn five_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Utf8, false),
            Field::new("snippet", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("start", DataType::UInt64, false),
            Field::new("end", DataType::UInt64, false),
        ]))
    }

    fn build_batch(rows: &[( &str, &str, f32, u64, u64 )]) -> RecordBatch {
        let mut sid = StringBuilder::new();
        let mut snip = StringBuilder::new();
        let mut sc = Float32Builder::new();
        let mut st = UInt64Builder::new();
        let mut e = UInt64Builder::new();
        for (a, b, c, d, f) in rows {
            sid.append_value(*a);
            snip.append_value(*b);
            sc.append_value(*c);
            st.append_value(*d);
            e.append_value(*f);
        }
        RecordBatch::try_new(
            five_col_schema(),
            vec![
                Arc::new(sid.finish()),
                Arc::new(snip.finish()),
                Arc::new(sc.finish()),
                Arc::new(st.finish()),
                Arc::new(e.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn ranks_by_score_desc_truncates() {
        let batch = build_batch(&[
            ("srcA", "alpha snippet", 0.25, 0, 10),
            ("srcB", "bravo snippet", 0.90, 5, 15),
            ("srcC", "charlie snippet", 0.60, 2, 8),
        ]);
        let out = rank_batch(&batch, 2, five_col_schema()).unwrap();
        assert_eq!(out.num_rows(), 2);
        let sid = out.column_by_name("source_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let score = out.column_by_name("score").unwrap()
            .as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(sid.value(0), "srcB");
        assert_eq!(sid.value(1), "srcC");
        assert!((score.value(0) - 0.90).abs() < 1e-6);
    }

    #[test]
    fn tie_breaks_source_id_then_start() {
        let batch = build_batch(&[
            ("srcB", "same score", 0.5, 9, 19),
            ("srcA", "same score", 0.5, 4, 14),
            ("srcA", "same score earlier", 0.5, 1, 11),
        ]);
        let out = rank_batch(&batch, 3, five_col_schema()).unwrap();
        let sid = out.column_by_name("source_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let start = out.column_by_name("start").unwrap()
            .as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(sid.value(0), "srcA");
        assert_eq!(start.value(0), 1);
        assert_eq!(sid.value(1), "srcA");
        assert_eq!(start.value(1), 4);
        assert_eq!(sid.value(2), "srcB");
    }
}

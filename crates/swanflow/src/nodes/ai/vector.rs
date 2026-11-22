//! Real vector nodes backed by an OpenAI-compatible /embeddings endpoint
//! (e.g. ollama bge-m3 on the LAN). Two nodes:
//!
//! - `ai.embedder`        : text rows -> (source_id, text, embedding JSON, dim)
//! - `ai.vector_search`   : embedded corpus + query -> top-k cosine-ranked rows
//!
//! Both follow the EntityExtractor execution pattern: Logical (UserDefinedLogicalNodeCore)
//! -> planner downcast -> Exec (CoalescePartitionsExec-wrapped) -> Stream with a
//! PollingInput/Processing/Done state machine.
//!
//! Config is resolved at propose_plan time: YAML config values take precedence,
//! env vars (EMBED_MODEL / EMBED_API_BASE / EMBED_API_KEY) are the fallback.
//! No config field is collected-but-ignored.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::{Stream, StreamExt};
use serde_json::Value as JsonValue;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

// ---------------------------------------------------------------------------
// Shared endpoint config resolution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct EmbedEndpoint {
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
}

fn resolve_endpoint(config: &JsonValue) -> Result<EmbedEndpoint> {
    let base_url = config["api_base"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("EMBED_API_BASE").ok())
        .unwrap_or_else(|| "http://127.0.0.1:11434/v1".to_string());
    let model = config["model"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("EMBED_MODEL").ok())
        .unwrap_or_else(|| "bge-m3".to_string());
    let api_key = config["api_key"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("EMBED_API_KEY").ok());
    if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
        return Err(anyhow!(
            "embedding api_base must start with http:// or https://, got: {base_url}"
        ));
    }
    Ok(EmbedEndpoint {
        base_url,
        model,
        api_key,
    })
}

/// POST {base}/embeddings with a batch of texts; returns one Vec<f32> per input.
async fn embed_texts(endpoint: &EmbedEndpoint, texts: &[String]) -> Result<Vec<Vec<f32>>> {
    let mut url = endpoint.base_url.trim_end_matches('/').to_string();
    url.push_str("/embeddings");
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .no_proxy()
        .build()?;
    let mut request = client
        .post(&url)
        .json(&serde_json::json!({ "model": endpoint.model, "input": texts }));
    if let Some(key) = &endpoint.api_key {
        request = request.bearer_auth(key);
    }
    let response = request.send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!("embeddings API error ({status}) from {url}: {body}");
    }
    let parsed: JsonValue =
        serde_json::from_str(&body).map_err(|e| anyhow!("invalid embeddings JSON: {e}: {body}"))?;
    let data = parsed["data"]
        .as_array()
        .ok_or_else(|| anyhow!("embeddings response missing data[] array: {body}"))?;
    if data.len() != texts.len() {
        anyhow::bail!(
            "embeddings endpoint returned {} vectors for {} inputs",
            data.len(),
            texts.len()
        );
    }
    let mut by_index: Vec<Option<Vec<f32>>> = vec![None; texts.len()];
    for item in data {
        let idx = item["index"].as_u64().unwrap_or(0) as usize;
        let vec = item["embedding"]
            .as_array()
            .ok_or_else(|| anyhow!("embedding entry missing embedding[] array"))?
            .iter()
            .map(|v| v.as_f64().unwrap_or(0.0) as f32)
            .collect::<Vec<f32>>();
        if idx < by_index.len() {
            by_index[idx] = Some(vec);
        }
    }
    by_index
        .into_iter()
        .zip(texts.iter())
        .map(|(v, t)| {
            v.ok_or_else(|| anyhow!("embeddings response missing vector for input {t:?}"))
        })
        .collect()
}

fn extract_utf8_column(
    batch: &RecordBatch,
    column: Option<&str>,
) -> std::result::Result<(usize, Vec<String>), DataFusionError> {
    let schema = batch.schema();
    let idx = match column {
        Some(name) => schema
            .index_of(name)
            .map_err(|_| DataFusionError::Execution(format!("column '{name}' not found")))?,
        None => schema
            .fields()
            .iter()
            .position(|f| {
                matches!(
                    f.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                )
            })
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "ai.embedder input has no string column to embed".to_string(),
                )
            })?,
    };
    let col = batch.column(idx);
    // Utf8View defense (family bug #10/#16a): cast to Utf8 before downcast.
    let col = if matches!(col.data_type(), DataType::Utf8View | DataType::LargeUtf8) {
        Arc::new(cast(col, &DataType::Utf8).map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to cast embedding input column to Utf8: {e}"
            ))
        })?)
    } else {
        col.clone()
    };
    let arr = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("embedding input column is not Utf8".into()))?;
    Ok((
        idx,
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect(),
    ))
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len().min(b.len()) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom > 0.0 {
        dot / denom
    } else {
        0.0
    }
}

// ---------------------------------------------------------------------------
// Output schemas
// ---------------------------------------------------------------------------

lazy_static::lazy_static! {
    static ref EMBED_SCHEMA: SchemaRef =
        Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("embedding", DataType::Utf8, false),
            Field::new("embedding_dim", DataType::Int64, false),
        ]));
    static ref EMBED_DF_SCHEMA: DFSchemaRef =
        Arc::new(datafusion::common::DFSchema::try_from(EMBED_SCHEMA.as_ref().clone()).unwrap());
    static ref SEARCH_SCHEMA: SchemaRef =
        Arc::new(Schema::new(vec![
            Field::new("rank", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
            Field::new("source_id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("query", DataType::Utf8, false),
        ]));
    static ref SEARCH_DF_SCHEMA: DFSchemaRef =
        Arc::new(datafusion::common::DFSchema::try_from(SEARCH_SCHEMA.as_ref().clone()).unwrap());
}

// ---------------------------------------------------------------------------
// ai.embedder — NodeBehavior + Logical
// ---------------------------------------------------------------------------

pub struct EmbedderNode;

#[async_trait]
impl NodeBehavior for EmbedderNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.embedder".to_string(),
            display_name: "Embedder".to_string(),
            description: "Generate real embeddings for text rows via an OpenAI-compatible /embeddings endpoint.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Rows containing at least one string column".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "source_id, text, embedding (JSON array), embedding_dim".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "model": { "type": "string" },
                    "api_base": { "type": "string" },
                    "api_key": { "type": "string" },
                    "column": { "type": "string", "description": "Text column to embed; default: first string column" },
                    "batch_size": { "type": "integer", "minimum": 1, "default": 32 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        resolve_endpoint(config)?;
        if let Some(size) = config["batch_size"].as_u64() {
            if size == 0 {
                return Err(anyhow!("batch_size must be greater than zero"));
            }
        } else if config.get("batch_size").is_some() {
            return Err(anyhow!("batch_size must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("ai.embedder requires one input"));
        }
        let endpoint = resolve_endpoint(config)?;
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(EmbedderLogical {
                input: inputs[0].clone(),
                endpoint,
                column: config["column"].as_str().map(str::to_string),
                batch_size: config["batch_size"].as_u64().unwrap_or(32) as usize,
            }),
        })])
    }
}

#[derive(Clone)]
pub struct EmbedderLogical {
    pub input: LogicalPlan,
    pub endpoint: EmbedEndpoint,
    pub column: Option<String>,
    pub batch_size: usize,
}

impl fmt::Debug for EmbedderLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbedderLogical")
            .field("model", &self.endpoint.model)
            .field("column", &self.column)
            .finish()
    }
}

impl PartialEq for EmbedderLogical {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.endpoint.base_url == other.endpoint.base_url
            && self.endpoint.model == other.endpoint.model
            && self.column == other.column
            && self.batch_size == other.batch_size
    }
}
impl Eq for EmbedderLogical {}
impl std::hash::Hash for EmbedderLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.base_url.hash(state);
        self.endpoint.model.hash(state);
        self.column.hash(state);
        self.batch_size.hash(state);
    }
}
impl PartialOrd for EmbedderLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.endpoint.model.cmp(&other.endpoint.model))
    }
}

impl UserDefinedLogicalNodeCore for EmbedderLogical {
    fn name(&self) -> &str {
        "Embedder"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &EMBED_DF_SCHEMA
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Embedder(model={})", self.endpoint.model)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            input: inputs[0].clone(),
            endpoint: self.endpoint.clone(),
            column: self.column.clone(),
            batch_size: self.batch_size,
        })
    }
}

// ---------------------------------------------------------------------------
// ai.embedder — Exec + Stream
// ---------------------------------------------------------------------------

pub struct EmbedderExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub endpoint: EmbedEndpoint,
    pub column: Option<String>,
    pub batch_size: usize,
    cache: PlanProperties,
}

impl EmbedderExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        endpoint: EmbedEndpoint,
        column: Option<String>,
        batch_size: usize,
    ) -> Self {
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let cache = PlanProperties::new(
            EquivalenceProperties::new(EMBED_SCHEMA.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            endpoint,
            column,
            batch_size: batch_size.max(1),
            cache,
        }
    }
}

impl fmt::Debug for EmbedderExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbedderExec")
            .field("model", &self.endpoint.model)
            .finish()
    }
}

impl DisplayAs for EmbedderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EmbedderExec(model={})", self.endpoint.model)
    }
}

impl ExecutionPlan for EmbedderExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        EMBED_SCHEMA.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.endpoint.clone(),
            self.column.clone(),
            self.batch_size,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(EmbeddingStream {
            input,
            endpoint: self.endpoint.clone(),
            column: self.column.clone(),
            batch_size: self.batch_size,
            buffer: Vec::new(),
            state: VectorStreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "EmbedderExec"
    }
}

enum VectorStreamState {
    PollingInput,
    Processing(BoxFuture<'static, std::result::Result<RecordBatch, DataFusionError>>),
    Done,
}

struct EmbeddingStream {
    input: SendableRecordBatchStream,
    endpoint: EmbedEndpoint,
    column: Option<String>,
    batch_size: usize,
    buffer: Vec<RecordBatch>,
    state: VectorStreamState,
}

impl Stream for EmbeddingStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Match on the discriminant only (no binding) so no borrow of
            // self.state is held while self.input / self.buffer are accessed.
            match self.state {
                VectorStreamState::PollingInput => match self.input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => self.buffer.push(batch),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        let endpoint = self.endpoint.clone();
                        let column = self.column.clone();
                        let batch_size = self.batch_size;
                        let batches = std::mem::take(&mut self.buffer);
                        let fut = async move {
                            embed_all_batches(batches, &endpoint, column.as_deref(), batch_size)
                                .await
                        };
                        self.state = VectorStreamState::Processing(Box::pin(fut));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                VectorStreamState::Processing(..) => {
                    let fut = match &mut self.state {
                        VectorStreamState::Processing(fut) => fut,
                        _ => unreachable!("checked discriminant above"),
                    };
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            self.state = VectorStreamState::Done;
                            return Poll::Ready(Some(result));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                VectorStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for EmbeddingStream {
    fn schema(&self) -> SchemaRef {
        EMBED_SCHEMA.clone()
    }
}

async fn embed_all_batches(
    batches: Vec<RecordBatch>,
    endpoint: &EmbedEndpoint,
    column: Option<&str>,
    batch_size: usize,
) -> std::result::Result<RecordBatch, DataFusionError> {
    let mut texts: Vec<String> = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let (_, col_texts) = extract_utf8_column(batch, column)?;
        texts.extend(col_texts);
    }
    let total = texts.len();
    eprintln!(
        "  [EMBED] {total} texts -> {} ({})",
        endpoint.model, endpoint.base_url
    );
    let mut embeddings: Vec<Vec<f32>> = Vec::with_capacity(total);
    for chunk_start in (0..total).step_by(batch_size) {
        let chunk_end = (chunk_start + batch_size).min(total);
        let chunk = &texts[chunk_start..chunk_end];
        let vectors = embed_texts(endpoint, chunk)
            .await
            .map_err(|e| DataFusionError::Execution(format!("ai.embedder call failed: {e}")))?;
        embeddings.extend(vectors);
    }
    let dim = embeddings.first().map(|v| v.len()).unwrap_or(0) as i64;
    let mut source_ids = datafusion::arrow::array::StringBuilder::new();
    let mut text_builder = datafusion::arrow::array::StringBuilder::new();
    let mut emb_builder = datafusion::arrow::array::StringBuilder::new();
    let mut dim_builder = datafusion::arrow::array::Int64Builder::new();
    for (i, (text, vector)) in texts.iter().zip(embeddings.iter()).enumerate() {
        source_ids.append_value(format!("row_{i:06}"));
        text_builder.append_value(text);
        let json_vec: Vec<f64> = vector.iter().map(|v| *v as f64).collect();
        emb_builder.append_value(serde_json::to_string(&json_vec).unwrap_or_default());
        dim_builder.append_value(vector.len() as i64);
    }
    let batch = RecordBatch::try_new(
        EMBED_SCHEMA.clone(),
        vec![
            Arc::new(source_ids.finish()),
            Arc::new(text_builder.finish()),
            Arc::new(emb_builder.finish()),
            Arc::new(dim_builder.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let _ = dim;
    Ok(batch)
}

// ---------------------------------------------------------------------------
// ai.vector_search — NodeBehavior + Logical
// ---------------------------------------------------------------------------

pub struct VectorSearchNode;

#[async_trait]
impl NodeBehavior for VectorSearchNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.vector_search".to_string(),
            display_name: "Vector Search".to_string(),
            description:
                "Embed the query with the same endpoint and rank corpus rows by cosine similarity."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "corpus".into(),
                description: "Embedded corpus from ai.embedder".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "rank, score, source_id, text, query".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "top_k": { "type": "integer", "minimum": 1, "default": 5 },
                    "model": { "type": "string" },
                    "api_base": { "type": "string" },
                    "api_key": { "type": "string" }
                },
                "required": ["query"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        resolve_endpoint(config)?;
        if config["query"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_none()
        {
            return Err(anyhow!("ai.vector_search requires a non-empty 'query'"));
        }
        if let Some(k) = config["top_k"].as_u64() {
            if k == 0 {
                return Err(anyhow!("top_k must be greater than zero"));
            }
        } else if config.get("top_k").is_some() {
            return Err(anyhow!("top_k must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!(
                "ai.vector_search requires one input (embedded corpus)"
            ));
        }
        let endpoint = resolve_endpoint(config)?;
        let query = config["query"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("ai.vector_search requires a non-empty 'query'"))?
            .to_string();
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(VectorSearchLogical {
                input: inputs[0].clone(),
                endpoint,
                query,
                top_k: config["top_k"].as_u64().unwrap_or(5) as usize,
            }),
        })])
    }
}

#[derive(Clone)]
pub struct VectorSearchLogical {
    pub input: LogicalPlan,
    pub endpoint: EmbedEndpoint,
    pub query: String,
    pub top_k: usize,
}

impl fmt::Debug for VectorSearchLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorSearchLogical")
            .field("query", &self.query)
            .field("top_k", &self.top_k)
            .finish()
    }
}

impl PartialEq for VectorSearchLogical {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.endpoint.base_url == other.endpoint.base_url
            && self.endpoint.model == other.endpoint.model
            && self.query == other.query
            && self.top_k == other.top_k
    }
}
impl Eq for VectorSearchLogical {}
impl std::hash::Hash for VectorSearchLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.base_url.hash(state);
        self.endpoint.model.hash(state);
        self.query.hash(state);
        self.top_k.hash(state);
    }
}
impl PartialOrd for VectorSearchLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.query.cmp(&other.query))
    }
}

impl UserDefinedLogicalNodeCore for VectorSearchLogical {
    fn name(&self) -> &str {
        "VectorSearch"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &SEARCH_DF_SCHEMA
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VectorSearch(query={:?}, top_k={})",
            self.query, self.top_k
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            input: inputs[0].clone(),
            endpoint: self.endpoint.clone(),
            query: self.query.clone(),
            top_k: self.top_k,
        })
    }
}

// ---------------------------------------------------------------------------
// ai.vector_search — Exec + Stream
// ---------------------------------------------------------------------------

pub struct VectorSearchExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub endpoint: EmbedEndpoint,
    pub query: String,
    pub top_k: usize,
    cache: PlanProperties,
}

impl VectorSearchExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        endpoint: EmbedEndpoint,
        query: String,
        top_k: usize,
    ) -> Self {
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let cache = PlanProperties::new(
            EquivalenceProperties::new(SEARCH_SCHEMA.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            endpoint,
            query,
            top_k: top_k.max(1),
            cache,
        }
    }
}

impl fmt::Debug for VectorSearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorSearchExec")
            .field("query", &self.query)
            .finish()
    }
}

impl DisplayAs for VectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VectorSearchExec(query={:?})", self.query)
    }
}

impl ExecutionPlan for VectorSearchExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        SEARCH_SCHEMA.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.endpoint.clone(),
            self.query.clone(),
            self.top_k,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(VectorSearchStream {
            input,
            endpoint: self.endpoint.clone(),
            query: self.query.clone(),
            top_k: self.top_k,
            buffer: Vec::new(),
            state: VectorSearchState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "VectorSearchExec"
    }
}

enum VectorSearchState {
    PollingInput,
    Processing(BoxFuture<'static, std::result::Result<RecordBatch, DataFusionError>>),
    Done,
}

struct VectorSearchStream {
    input: SendableRecordBatchStream,
    endpoint: EmbedEndpoint,
    query: String,
    top_k: usize,
    buffer: Vec<RecordBatch>,
    state: VectorSearchState,
}

impl Stream for VectorSearchStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // Discriminant-only match — see EmbeddingStream for the borrow rationale.
            match self.state {
                VectorSearchState::PollingInput => match self.input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => self.buffer.push(batch),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        let endpoint = self.endpoint.clone();
                        let query = self.query.clone();
                        let top_k = self.top_k;
                        let batches = std::mem::take(&mut self.buffer);
                        let fut = async move {
                            vector_search_all(batches, &endpoint, &query, top_k).await
                        };
                        self.state = VectorSearchState::Processing(Box::pin(fut));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                VectorSearchState::Processing(..) => {
                    let fut = match &mut self.state {
                        VectorSearchState::Processing(fut) => fut,
                        _ => unreachable!("checked discriminant above"),
                    };
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            self.state = VectorSearchState::Done;
                            return Poll::Ready(Some(result));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                VectorSearchState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for VectorSearchStream {
    fn schema(&self) -> SchemaRef {
        SEARCH_SCHEMA.clone()
    }
}

async fn vector_search_all(
    batches: Vec<RecordBatch>,
    endpoint: &EmbedEndpoint,
    query: &str,
    top_k: usize,
) -> std::result::Result<RecordBatch, DataFusionError> {
    // Collect corpus rows: source_id / text / embedding from ai.embedder output.
    let mut source_ids: Vec<String> = Vec::new();
    let mut texts: Vec<String> = Vec::new();
    let mut vectors: Vec<Vec<f32>> = Vec::new();
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let schema = batch.schema();
        let sid_idx = schema.index_of("source_id").map_err(|_| {
            DataFusionError::Execution(
                "ai.vector_search input must come from ai.embedder (missing source_id column)"
                    .into(),
            )
        })?;
        let text_idx = schema.index_of("text").map_err(|_| {
            DataFusionError::Execution(
                "ai.vector_search input must come from ai.embedder (missing text column)".into(),
            )
        })?;
        let emb_idx = schema.index_of("embedding").map_err(|_| {
            DataFusionError::Execution(
                "ai.vector_search input must come from ai.embedder (missing embedding column)"
                    .into(),
            )
        })?;
        for idx in [sid_idx, text_idx, emb_idx] {
            let col = batch.column(idx);
            let col =
                if matches!(col.data_type(), DataType::Utf8View | DataType::LargeUtf8) {
                    Arc::new(cast(col, &DataType::Utf8).map_err(|e| {
                        DataFusionError::Execution(format!("cast to Utf8 failed: {e}"))
                    })?)
                } else {
                    col.clone()
                };
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("expected Utf8 column".into()))?;
            for i in 0..arr.len() {
                match idx {
                    x if x == sid_idx => source_ids.push(arr.value(i).to_string()),
                    x if x == text_idx => texts.push(arr.value(i).to_string()),
                    _ => {
                        let parsed: Vec<f32> = serde_json::from_str(arr.value(i)).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "embedding column is not valid JSON array: {e}"
                            ))
                        })?;
                        vectors.push(parsed);
                    }
                }
            }
        }
    }
    if vectors.is_empty() {
        return Err(DataFusionError::Execution(
            "ai.vector_search received an empty corpus".into(),
        ));
    }
    eprintln!(
        "  [VECSEARCH] query={:?} corpus={} docs -> {}",
        query,
        vectors.len(),
        endpoint.base_url
    );
    let query_vec = embed_texts(endpoint, &[query.to_string()])
        .await
        .map_err(|e| DataFusionError::Execution(format!("query embedding failed: {e}")))?
        .remove(0);
    let mut ranked: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| (i, cosine(&query_vec, v)))
        .collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(top_k);

    let mut rank_builder = datafusion::arrow::array::Int64Builder::new();
    let mut score_builder = datafusion::arrow::array::Float64Builder::new();
    let mut sid_builder = datafusion::arrow::array::StringBuilder::new();
    let mut text_builder = datafusion::arrow::array::StringBuilder::new();
    let mut query_builder = datafusion::arrow::array::StringBuilder::new();
    for (rank, (i, score)) in ranked.into_iter().enumerate() {
        rank_builder.append_value((rank + 1) as i64);
        score_builder.append_value(score as f64);
        sid_builder.append_value(source_ids.get(i).cloned().unwrap_or_default());
        text_builder.append_value(texts.get(i).cloned().unwrap_or_default());
        query_builder.append_value(query);
    }
    RecordBatch::try_new(
        SEARCH_SCHEMA.clone(),
        vec![
            Arc::new(rank_builder.finish()),
            Arc::new(score_builder.finish()),
            Arc::new(sid_builder.finish()),
            Arc::new(text_builder.finish()),
            Arc::new(query_builder.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

// keep Int64Array/Float64Array imports referenced (used in schema fields)
#[allow(dead_code)]
fn _type_assertions(a: &Int64Array, f: &Float64Array) -> usize {
    a.len() + f.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_identical_and_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        let c = vec![0.0, 1.0, 0.0];
        assert!((cosine(&a, &b) - 1.0).abs() < 1e-6);
        assert!(cosine(&a, &c).abs() < 1e-6);
        // zero vector must not NaN
        assert!(cosine(&vec![0.0; 3], &a).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_scale_invariant() {
        let a = vec![0.2, 0.4, -0.5];
        let b: Vec<f32> = a.iter().map(|v| v * 17.0).collect();
        assert!((cosine(&a, &b) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn test_resolve_endpoint_config_overrides_env_fallback() {
        let cfg = serde_json::json!({
            "api_base": "http://10.10.10.100:11434/v1",
            "model": "bge-m3"
        });
        let ep = resolve_endpoint(&cfg).unwrap();
        assert_eq!(ep.base_url, "http://10.10.10.100:11434/v1");
        assert_eq!(ep.model, "bge-m3");
        assert!(ep.api_key.is_none());

        // default when nothing set
        std::env::remove_var("EMBED_API_BASE");
        std::env::remove_var("EMBED_MODEL");
        let ep = resolve_endpoint(&serde_json::json!({})).unwrap();
        assert_eq!(ep.base_url, "http://127.0.0.1:11434/v1");
        assert_eq!(ep.model, "bge-m3");

        // invalid scheme fails loudly
        assert!(resolve_endpoint(&serde_json::json!({ "api_base": "ftp://x" })).is_err());
    }

    #[test]
    fn test_vector_search_requires_query() {
        let node = VectorSearchNode;
        assert!(node
            .validate(&serde_json::json!({ "query": "  " }))
            .is_err());
        assert!(node.validate(&serde_json::json!({})).is_err());
        assert!(node
            .validate(&serde_json::json!({ "query": "胸痛", "top_k": 5 }))
            .is_ok());
        assert!(node
            .validate(&serde_json::json!({ "query": "x", "top_k": 0 }))
            .is_err());
    }

    #[test]
    fn test_embedder_batch_size_validation() {
        let node = EmbedderNode;
        assert!(node
            .validate(&serde_json::json!({ "batch_size": 0 }))
            .is_err());
        assert!(node
            .validate(&serde_json::json!({ "batch_size": "x" }))
            .is_err());
        assert!(node
            .validate(&serde_json::json!({ "batch_size": 8 }))
            .is_ok());
    }
}

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, Float64Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
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
use datafusion::prelude::{DataFrame, SessionContext};
use futures::future::BoxFuture;
use futures::Stream;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::nodes::ai::providers::LlmProvider;
use crate::nodes::common::hash_string_to_i64;
use crate::nodes::{GraphSchemas, NodeBehavior, NodeSpec, PortSpec};

const GAP_ANALYSIS_PROMPT: &str = r#"
Analyze the following Knowledge Graph summary (nodes and edges).
Identify the most important missing information or "information gaps" that prevent a comprehensive understanding of the topic.
Suggest exactly 3 targeted search queries to fill these gaps.
Return the result in JSON format:
{
  "gaps": ["description of gap 1", "..."],
  "suggested_queries": ["query 1", "query 2", "query 3"]
}
"#;

use std::collections::HashSet;
use std::sync::Mutex;

#[derive(Debug, Clone, Default)]
pub struct ResearchState {
    pub visited_urls: HashSet<String>,
    pub visited_queries: HashSet<String>,
    pub current_depth: usize,
    pub max_depth: usize,
}

impl ResearchState {
    pub fn new(max_depth: usize) -> Self {
        Self {
            max_depth,
            ..Default::default()
        }
    }

    pub fn should_continue(&self) -> bool {
        self.current_depth < self.max_depth
    }

    pub fn is_visited_url(&self, url: &str) -> bool {
        self.visited_urls.contains(url)
    }

    pub fn is_visited_query(&self, query: &str) -> bool {
        self.visited_queries.contains(query)
    }

    pub fn visit_url(&mut self, url: String) {
        self.visited_urls.insert(url);
    }

    pub fn visit_query(&mut self, query: String) {
        self.visited_queries.insert(query);
    }
}

pub struct EntityExtractorNode {
    pub provider: Arc<dyn LlmProvider>,
}

#[async_trait]
impl NodeBehavior for EntityExtractorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.entity_extractor".to_string(),
            display_name: "Entity Extractor".to_string(),
            description: "Extract nodes and edges from text using LLM".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Text chunks".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes".into(),
                    description: "Extracted entities".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges".into(),
                    description: "Extracted relationships".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "entity_types": { "type": "array", "items": { "type": "string" } },
                    "relation_types": { "type": "array", "items": { "type": "string" } }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("EntityExtractor requires one input"));
        }

        let node = Arc::new(EntityExtractorLogical {
            input: inputs[0].clone(),
            entity_types: config["entity_types"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
            relation_types: config["relation_types"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
            provider: self.provider.clone(),
        });

        Ok(vec![
            LogicalPlan::Extension(Extension {
                node: Arc::new(EntityPortLogical {
                    base: node.clone(),
                    port: 0,
                }),
            }),
            LogicalPlan::Extension(Extension {
                node: Arc::new(EntityPortLogical {
                    base: node.clone(),
                    port: 1,
                }),
            }),
        ])
    }
}

#[derive(Clone)]
pub struct EntityExtractorLogical {
    pub input: LogicalPlan,
    pub entity_types: Vec<String>,
    pub relation_types: Vec<String>,
    pub provider: Arc<dyn LlmProvider>,
}

impl fmt::Debug for EntityExtractorLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityExtractorLogical")
            .field("entity_types", &self.entity_types)
            .field("relation_types", &self.relation_types)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct EntityPortLogical {
    pub base: Arc<EntityExtractorLogical>,
    pub port: usize,
}

impl PartialEq for EntityPortLogical {
    fn eq(&self, other: &Self) -> bool {
        self.port == other.port
            && self.base.entity_types == other.base.entity_types
            && self.base.relation_types == other.base.relation_types
    }
}

impl Eq for EntityPortLogical {}

impl std::hash::Hash for EntityPortLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.port.hash(state);
        self.base.entity_types.hash(state);
        self.base.relation_types.hash(state);
    }
}

impl PartialOrd for EntityPortLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.port.partial_cmp(&other.port)
    }
}

impl UserDefinedLogicalNodeCore for EntityPortLogical {
    fn name(&self) -> &str {
        "EntityPort"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.base.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref NODE_DF_SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(GraphSchemas::node_schema().as_ref().clone()).unwrap());
            static ref EDGE_DF_SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(GraphSchemas::edge_schema().as_ref().clone()).unwrap());
        }
        if self.port == 0 {
            &NODE_DF_SCHEMA
        } else {
            &EDGE_DF_SCHEMA
        }
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EntityPort(port={})", self.port)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        let mut base = self.base.as_ref().clone();
        base.input = inputs[0].clone();
        Ok(Self {
            base: Arc::new(base),
            port: self.port,
        })
    }
}

pub struct EntityExtractorExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub entity_types: Vec<String>,
    pub relation_types: Vec<String>,
    pub provider: Arc<dyn LlmProvider>,
    pub port: usize,
    cache: PlanProperties,
}

impl fmt::Debug for EntityExtractorExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityExtractorExec")
            .field("port", &self.port)
            .finish()
    }
}

impl EntityExtractorExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        entity_types: Vec<String>,
        relation_types: Vec<String>,
        provider: Arc<dyn LlmProvider>,
        port: usize,
    ) -> Self {
        // CRITICAL: coalesce input partitions to 1 — without this, multi-partition
        // inputs (e.g. UnionExec from a UNION ALL source) only feed partition 0's
        // rows into extraction; other partitions' texts are silently dropped.
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let schema = if port == 0 {
            GraphSchemas::node_schema()
        } else {
            GraphSchemas::edge_schema()
        };
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            entity_types,
            relation_types,
            provider,
            port,
            cache,
        }
    }
}

impl DisplayAs for EntityExtractorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EntityExtractorExec(port={})", self.port)
    }
}

impl ExecutionPlan for EntityExtractorExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        if self.port == 0 {
            GraphSchemas::node_schema()
        } else {
            GraphSchemas::edge_schema()
        }
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
            self.entity_types.clone(),
            self.relation_types.clone(),
            self.provider.clone(),
            self.port,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(EntityExtractionStream {
            input,
            provider: self.provider.clone(),
            port: self.port,
            entity_types: self.entity_types.clone(),
            relation_types: self.relation_types.clone(),
            state: StreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "EntityExtractorExec"
    }
}

enum StreamState {
    PollingInput,
    Processing(BoxFuture<'static, std::result::Result<RecordBatch, DataFusionError>>),
    Done,
}

struct EntityExtractionStream {
    input: SendableRecordBatchStream,
    provider: Arc<dyn LlmProvider>,
    port: usize,
    entity_types: Vec<String>,
    relation_types: Vec<String>,
    state: StreamState,
}

impl Stream for EntityExtractionStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::PollingInput => match self.input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        let provider = self.provider.clone();
                        let port = self.port;
                        let entity_types = self.entity_types.clone();
                        let relation_types = self.relation_types.clone();

                        let fut = async move {
                            let mut node_id_builder = Int64Builder::new();
                            let mut node_name_builder = StringBuilder::new();
                            let mut node_label_builder = StringBuilder::new();
                            let mut node_props_builder = StringBuilder::new();

                            let mut edge_src_builder = Int64Builder::new();
                            let mut edge_dst_builder = Int64Builder::new();
                            let mut edge_type_builder = StringBuilder::new();
                            let mut edge_weight_builder = Float64Builder::new();

                            // The extractor reads the FIRST column as text.
                            // Historically this was an .unwrap() that panicked
                            // inside a tokio task ("Join Error / task panicked")
                            // whenever column 0 was not a string (e.g. an id
                            // column first). Fail with a clear message instead.
                            // DuckDB-backed sources commonly surface Utf8View
                            // (string view) buffers; cast those to plain Utf8
                            // so string columns are accepted as-is (same policy
                            // as ai.community_batches, see communities.rs).
                            let text_array: ArrayRef = if let Some(_sa) =
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                            {
                                std::sync::Arc::clone(batch.column(0))
                            } else {
                                let schema = batch.schema();
                                let f = schema.field(0);
                                if !matches!(
                                    f.data_type(),
                                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                                ) {
                                    return Err(DataFusionError::Execution(format!(
                                            "ai.entity_extractor expects column 0 to be a string, got {} ({:?}). Put the text column first.",
                                            f.data_type(),
                                            f.name()
                                        )));
                                }
                                datafusion::arrow::compute::cast(batch.column(0), &DataType::Utf8)
                                    .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "ai.entity_extractor: cannot cast column 0 {} to Utf8: {e}",
                                        f.name()
                                    ))
                                })?
                            };
                            let text_col = text_array
                                .as_any()
                                .downcast_ref::<datafusion::arrow::array::StringArray>()
                                .ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "ai.entity_extractor: column 0 is not a string array"
                                            .to_string(),
                                    )
                                })?;

                            for i in 0..batch.num_rows() {
                                let text = text_col.value(i);
                                let guidance = if !entity_types.is_empty() {
                                    format!(
                                        "\nFocus on entity types: {}. Relation types: {}.",
                                        entity_types.join(", "),
                                        if relation_types.is_empty() {
                                            "any".to_string()
                                        } else {
                                            relation_types.join(", ")
                                        }
                                    )
                                } else {
                                    String::new()
                                };
                                let prompt = format!(
                                        "Extract entities and relationships from this text.\nReturn JSON format:\n{{\n  \"nodes\": [{{ \"name\": \"...\", \"label\": \"...\", \"properties\": {{}} }}],\n  \"edges\": [{{ \"source\": \"...\", \"target\": \"...\", \"type\": \"...\", \"weight\": 1.0 }}]\n}}\nBe concise: at most 8 nodes and 10 edges. Output ONLY the JSON object, no markdown fences, no commentary.{guidance}\nText: {}",
                                        text
                                    );

                                // Retry loop: reasoning-capable servers occasionally return
                                // truncated JSON with finish_reason=stop (observed on SGLang).
                                // Parse failures trigger up to 3 re-calls before failing.
                                let mut json: Option<JsonValue> = None;
                                let mut last_err = String::new();
                                for attempt in 0..3 {
                                    // Identical retries are useless at temperature=0 (deterministic
                                    // truncation observed on SGLang reasoning models). Vary the
                                    // prompt on retries to break the degenerate output.
                                    let attempt_prompt = if attempt == 0 {
                                        prompt.clone()
                                    } else {
                                        format!(
                                            "{prompt}\nIMPORTANT: Output the COMPLETE JSON object. It must be valid JSON ending with the closing brace }}. Do not stop early."
                                        )
                                    };
                                    let result =
                                        provider.complete(&attempt_prompt).await.map_err(|e| {
                                            DataFusionError::Execution(format!(
                                            "Entity extraction LLM call failed for row {i}: {e}"
                                        ))
                                        })?;
                                    let json_str =
                                        crate::nodes::ai::ConfigWizard::extract_json_from_response(
                                            &result,
                                        )
                                        .unwrap_or(&result);
                                    match serde_json::from_str(json_str) {
                                        Ok(v) => {
                                            if attempt > 0 {
                                                eprintln!(
                                                    "  [LLM] row {i}: JSON parsed on retry {attempt}"
                                                );
                                            }
                                            json = Some(v);
                                            break;
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "  [LLM] row {i}: invalid JSON (attempt {}): {} | extracted tail: {:?}",
                                                attempt + 1,
                                                e,
                                                json_str.chars().rev().take(120).collect::<String>()
                                            );
                                            last_err = format!("{e}");
                                        }
                                    }
                                }
                                let json: JsonValue = json.ok_or_else(|| {
                                    DataFusionError::Execution(format!(
                                        "Entity extraction returned invalid JSON for row {i} after 3 attempts: {last_err}"
                                    ))
                                })?;
                                if let Some(nodes) = json["nodes"].as_array() {
                                    for n in nodes {
                                        let name = n["name"].as_str().unwrap_or("");
                                        node_id_builder.append_value(hash_string_to_i64(name));
                                        node_name_builder.append_value(name);
                                        node_label_builder
                                            .append_value(n["label"].as_str().unwrap_or(""));
                                        node_props_builder
                                            .append_value(n["properties"].to_string());
                                    }
                                }
                                if let Some(edges) = json["edges"].as_array() {
                                    for e in edges {
                                        let src = e["source"].as_str().unwrap_or("");
                                        let dst = e["target"].as_str().unwrap_or("");
                                        edge_src_builder.append_value(hash_string_to_i64(src));
                                        edge_dst_builder.append_value(hash_string_to_i64(dst));
                                        edge_type_builder
                                            .append_value(e["type"].as_str().unwrap_or(""));
                                        edge_weight_builder
                                            .append_value(e["weight"].as_f64().unwrap_or(1.0));
                                    }
                                }
                            }

                            if port == 0 {
                                let batch = RecordBatch::try_new(
                                    GraphSchemas::node_schema(),
                                    vec![
                                        Arc::new(node_id_builder.finish()),
                                        Arc::new(node_name_builder.finish()),
                                        Arc::new(node_label_builder.finish()),
                                        Arc::new(node_props_builder.finish()),
                                    ],
                                )
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                Ok(batch)
                            } else {
                                let batch = RecordBatch::try_new(
                                    GraphSchemas::edge_schema(),
                                    vec![
                                        Arc::new(edge_src_builder.finish()),
                                        Arc::new(edge_dst_builder.finish()),
                                        Arc::new(edge_type_builder.finish()),
                                        Arc::new(edge_weight_builder.finish()),
                                    ],
                                )
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                Ok(batch)
                            }
                        };
                        self.state = StreamState::Processing(Box::pin(fut));
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        self.state = StreamState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Processing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.state = StreamState::PollingInput;
                        return Poll::Ready(Some(result));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for EntityExtractionStream {
    fn schema(&self) -> SchemaRef {
        if self.port == 0 {
            GraphSchemas::node_schema()
        } else {
            GraphSchemas::edge_schema()
        }
    }
}

pub struct GraphRetrieverNode;

#[async_trait]
impl NodeBehavior for GraphRetrieverNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.graph_retriever".to_string(),
            display_name: "Graph Retriever".to_string(),
            description: "Retrieve multi-hop neighborhood from graph index".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "entities".into(),
                description: "Entities to search from".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "context".into(),
                description: "Textual context from graph".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "depth": { "type": "integer", "default": 1 },
                    "entity_name": { "type": "string", "description": "Anchor entity name to walk from (preferred over entity_id input)" }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("GraphRetriever requires entities input"));
        }

        let depth = config["depth"].as_u64().unwrap_or(1);
        let entity_name = config["entity_name"].as_str().map(|s| s.to_string());

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(GraphRetrieverLogical {
                input: inputs[0].clone(),
                depth,
                entity_name,
            }),
        })])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct GraphRetrieverLogical {
    pub input: LogicalPlan,
    pub depth: u64,
    pub entity_name: Option<String>,
}

impl UserDefinedLogicalNodeCore for GraphRetrieverLogical {
    fn name(&self) -> &str {
        "GraphRetriever"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(Schema::new(vec![
                Field::new("context", DataType::Utf8, true),
            ])).unwrap());
        }
        &SCHEMA
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphRetriever(depth={})", self.depth)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            input: inputs[0].clone(),
            depth: self.depth,
            entity_name: self.entity_name.clone(),
        })
    }
}

pub struct GraphRetrieverExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub depth: u64,
    pub entity_name: Option<String>,
    pub cache_dir: String,
    cache: PlanProperties,
}

impl fmt::Debug for GraphRetrieverExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GraphRetrieverExec")
            .field("depth", &self.depth)
            .finish()
    }
}

impl GraphRetrieverExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        depth: u64,
        entity_name: Option<String>,
        cache_dir: String,
    ) -> Self {
        // CRITICAL: coalesce input partitions to 1 (same partition-drop hazard as
        // EntityExtractorExec) so all input rows reach the retrieval anchor logic.
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "context",
            DataType::Utf8,
            true,
        )]));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            depth,
            entity_name,
            cache_dir,
            cache,
        }
    }
}

impl DisplayAs for GraphRetrieverExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphRetrieverExec(depth={})", self.depth)
    }
}

impl ExecutionPlan for GraphRetrieverExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "context",
            DataType::Utf8,
            true,
        )]))
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
            self.depth,
            self.entity_name.clone(),
            self.cache_dir.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(GraphRetrievalStream {
            input,
            depth: self.depth,
            entity_name: self.entity_name.clone(),
            cache_dir: self.cache_dir.clone(),
            state: StreamState::PollingInput,
            ran_config_retrieval: false,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "GraphRetrieverExec"
    }
}

struct GraphRetrievalStream {
    input: SendableRecordBatchStream,
    depth: u64,
    entity_name: Option<String>,
    cache_dir: String,
    state: StreamState,
    ran_config_retrieval: bool,
}

impl Stream for GraphRetrievalStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::PollingInput => match self.input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        let cache_dir = self.cache_dir.clone();
                        let depth = self.depth;
                        let entity_name = self.entity_name.clone();
                        let fut = async move {
                            let mut context_builder = StringBuilder::new();
                            let metadata_path = format!("{}/metadata.duckdb", cache_dir);

                            // Anchor selection: config entity_name (preferred) or entity_id from input rows
                            let anchors: Vec<String> = if let Some(name) = &entity_name {
                                vec![format!(
                                    "SELECT id, 0 FROM meta.__graph_nodes WHERE name = '{}'",
                                    name.replace('\'', "''")
                                )]
                            } else {
                                let entity_ids = batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .ok_or_else(|| {
                                        DataFusionError::Execution(
                                            "GraphRetriever input column 0 must be Int64 entity ids, or set config 'entity_name'"
                                                .to_string(),
                                        )
                                    })?;
                                (0..batch.num_rows())
                                    .map(|i| format!("SELECT {}, 0", entity_ids.value(i)))
                                    .collect()
                            };

                            for anchor in anchors {
                                // Recursive multi-hop walk (depth honored)
                                let query = format!(
                                    "WITH RECURSIVE walk(entity_id, hop) AS (
                                        {anchor}
                                        UNION
                                        SELECT CASE WHEN e.source_id = w.entity_id THEN e.target_id ELSE e.source_id END, w.hop + 1
                                        FROM walk w
                                        JOIN meta.__graph_edges e
                                          ON e.source_id = w.entity_id OR e.target_id = w.entity_id
                                        WHERE w.hop < {depth}
                                     )
                                     SELECT n.name, n.label, n.properties
                                     FROM walk w JOIN meta.__graph_nodes n ON n.id = w.entity_id"
                                );

                                let batches = crate::nodes::io::DuckDBHelper::query_arrow(
                                    ":memory:",
                                    &query,
                                    &[],
                                    &Some(format!(
                                        "ATTACH '{}' AS meta (TYPE duckdb);",
                                        metadata_path
                                    )),
                                )
                                .await
                                .map_err(|e| {
                                    DataFusionError::Execution(format!(
                                        "Graph retrieval query failed: {e}"
                                    ))
                                })?;
                                for b in batches {
                                    // DuckDB may return Utf8View/LargeUtf8/Dictionary string
                                    // columns depending on version — cast explicitly
                                    // instead of unwrap-panicking on downcast.
                                    let names_col = datafusion::arrow::compute::cast(
                                        b.column(0),
                                        &DataType::Utf8,
                                    )
                                    .map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "Graph retrieval: cast name column failed: {e}"
                                        ))
                                    })?;
                                    let labels_col = datafusion::arrow::compute::cast(
                                        b.column(1),
                                        &DataType::Utf8,
                                    )
                                    .map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "Graph retrieval: cast label column failed: {e}"
                                        ))
                                    })?;
                                    let names = names_col
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .ok_or_else(|| {
                                            DataFusionError::Execution(
                                                "Graph retrieval: name column not string".into(),
                                            )
                                        })?;
                                    let labels = labels_col
                                        .as_any()
                                        .downcast_ref::<StringArray>()
                                        .ok_or_else(|| {
                                            DataFusionError::Execution(
                                                "Graph retrieval: label column not string".into(),
                                            )
                                        })?;
                                    for j in 0..b.num_rows() {
                                        context_builder.append_value(format!(
                                            "Entity: {} ({})",
                                            names.value(j),
                                            labels.value(j)
                                        ));
                                    }
                                }
                            }

                            let batch = RecordBatch::try_new(
                                Arc::new(Schema::new(vec![Field::new(
                                    "context",
                                    DataType::Utf8,
                                    true,
                                )])),
                                vec![Arc::new(context_builder.finish())],
                            )
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                            Ok(batch)
                        };
                        self.state = StreamState::Processing(Box::pin(fut));
                    }
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                    Poll::Ready(None) => {
                        // Config-anchored retrieval (entity_name) does not need input
                        // rows: upstream is typically GraphIndexWriter whose output is
                        // an EMPTY stream. Run the retrieval exactly once instead of
                        // short-circuiting to Done (which silently returned 0 rows).
                        if self.entity_name.is_some() && !self.ran_config_retrieval {
                            self.ran_config_retrieval = true;
                            let cache_dir = self.cache_dir.clone();
                            let depth = self.depth;
                            let entity_name = self.entity_name.clone();
                            let fut = async move {
                                let mut context_builder = StringBuilder::new();
                                let metadata_path = format!("{}/metadata.duckdb", cache_dir);
                                let anchors: Vec<String> = if let Some(name) = &entity_name {
                                    vec![format!(
                                        "SELECT id, 0 FROM meta.__graph_nodes WHERE name = '{}'",
                                        name.replace('\'', "''")
                                    )]
                                } else {
                                    vec![]
                                };
                                for anchor in anchors {
                                    let query = format!(
                                        "WITH RECURSIVE walk(entity_id, hop) AS (
                                            {anchor}
                                            UNION
                                            SELECT CASE WHEN e.source_id = w.entity_id THEN e.target_id ELSE e.source_id END, w.hop + 1
                                            FROM walk w
                                            JOIN meta.__graph_edges e
                                              ON e.source_id = w.entity_id OR e.target_id = w.entity_id
                                            WHERE w.hop < {depth}
                                         )
                                         SELECT n.name, n.label, n.properties
                                         FROM walk w JOIN meta.__graph_nodes n ON n.id = w.entity_id"
                                    );
                                    let batches = crate::nodes::io::DuckDBHelper::query_arrow(
                                        ":memory:",
                                        &query,
                                        &[],
                                        &Some(format!(
                                            "ATTACH '{}' AS meta (READ_ONLY, TYPE duckdb);",
                                            metadata_path
                                        )),
                                    )
                                    .await
                                    .map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "Graph retrieval query failed: {e}"
                                        ))
                                    })?;
                                    for b in batches {
                                        let names_col = datafusion::arrow::compute::cast(
                                            b.column(0),
                                            &DataType::Utf8,
                                        )
                                        .map_err(|e| {
                                            DataFusionError::Execution(format!(
                                                "Graph retrieval: cast name column failed: {e}"
                                            ))
                                        })?;
                                        let labels_col = datafusion::arrow::compute::cast(
                                            b.column(1),
                                            &DataType::Utf8,
                                        )
                                        .map_err(|e| {
                                            DataFusionError::Execution(format!(
                                                "Graph retrieval: cast label column failed: {e}"
                                            ))
                                        })?;
                                        let names = names_col
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .ok_or_else(|| {
                                                DataFusionError::Execution(
                                                    "Graph retrieval: name column not string"
                                                        .into(),
                                                )
                                            })?;
                                        let labels = labels_col
                                            .as_any()
                                            .downcast_ref::<StringArray>()
                                            .ok_or_else(|| {
                                                DataFusionError::Execution(
                                                    "Graph retrieval: label column not string"
                                                        .into(),
                                                )
                                            })?;
                                        for j in 0..b.num_rows() {
                                            context_builder.append_value(format!(
                                                "Entity: {} ({})",
                                                names.value(j),
                                                labels.value(j)
                                            ));
                                        }
                                    }
                                }
                                let batch = RecordBatch::try_new(
                                    Arc::new(Schema::new(vec![Field::new(
                                        "context",
                                        DataType::Utf8,
                                        true,
                                    )])),
                                    vec![Arc::new(context_builder.finish())],
                                )
                                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                Ok(batch)
                            };
                            self.state = StreamState::Processing(Box::pin(fut));
                            continue;
                        }
                        self.state = StreamState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Processing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        if self.entity_name.is_some() && self.ran_config_retrieval {
                            self.state = StreamState::Done;
                        } else {
                            self.state = StreamState::PollingInput;
                        }
                        return Poll::Ready(Some(result));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for GraphRetrievalStream {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "context",
            DataType::Utf8,
            true,
        )]))
    }
}

pub struct ContextFuserNode;

#[async_trait]
impl NodeBehavior for ContextFuserNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.context_fuser".to_string(),
            display_name: "Context Fuser".to_string(),
            description: "Merge and deduplicate context from vector and graph sources".to_string(),
            input_ports: vec![
                PortSpec {
                    id: 0,
                    name: "vector".into(),
                    description: "Vector search results".into(),
                },
                PortSpec {
                    id: 1,
                    name: "graph".into(),
                    description: "Graph retrieval results".into(),
                },
            ],
            output_ports: vec![PortSpec {
                id: 0,
                name: "context".into(),
                description: "Final fused context".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "strategy": { "type": "string", "enum": ["simple_concat", "rrf"], "default": "simple_concat" }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.len() < 2 {
            return Err(anyhow!(
                "ContextFuser requires at least two context sources"
            ));
        }

        let ctx = SessionContext::new();

        let v_df = DataFrame::new(ctx.state(), inputs[0].clone());
        let g_df = DataFrame::new(ctx.state(), inputs[1].clone());

        ctx.register_table("v_input", v_df.into_view())?;
        ctx.register_table("g_input", g_df.into_view())?;

        let fused = ctx
            .sql("SELECT context FROM v_input UNION SELECT context FROM g_input")
            .await?;

        Ok(vec![fused.into_optimized_plan()?])
    }
}

pub struct InformationGapNode {
    pub provider: Arc<dyn LlmProvider>,
}

#[async_trait]
impl NodeBehavior for InformationGapNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.information_gap_analyzer".to_string(),
            display_name: "Information Gap Analyzer".to_string(),
            description: "Analyze current graph and suggest follow-up research queries."
                .to_string(),
            input_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes".into(),
                    description: "Current Nodes".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges".into(),
                    description: "Current Edges".into(),
                },
            ],
            output_ports: vec![PortSpec {
                id: 0,
                name: "queries".into(),
                description: "Suggested search queries".into(),
            }],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.len() < 2 {
            return Err(anyhow!(
                "InformationGapAnalyzer requires nodes and edges input"
            ));
        }

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(InformationGapLogical {
                nodes_input: inputs[0].clone(),
                edges_input: inputs[1].clone(),
                provider: self.provider.clone(),
            }),
        })])
    }
}

#[derive(Clone)]
pub struct InformationGapLogical {
    pub nodes_input: LogicalPlan,
    pub edges_input: LogicalPlan,
    pub provider: Arc<dyn LlmProvider>,
}

impl fmt::Debug for InformationGapLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InformationGapLogical")
    }
}

impl PartialEq for InformationGapLogical {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.provider, &other.provider)
    }
}

impl Eq for InformationGapLogical {}

impl std::hash::Hash for InformationGapLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.provider) as *const ()).hash(state);
    }
}

impl PartialOrd for InformationGapLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (Arc::as_ptr(&self.provider) as *const ())
            .partial_cmp(&(Arc::as_ptr(&other.provider) as *const ()))
    }
}

impl UserDefinedLogicalNodeCore for InformationGapLogical {
    fn name(&self) -> &str {
        "InformationGap"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.nodes_input, &self.edges_input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(Schema::new(vec![
                Field::new("query", DataType::Utf8, true),
            ])).unwrap());
        }
        &SCHEMA
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InformationGap")
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            nodes_input: inputs[0].clone(),
            edges_input: inputs[1].clone(),
            provider: self.provider.clone(),
        })
    }
}

pub struct InformationGapExec {
    pub nodes_input: Arc<dyn ExecutionPlan>,
    pub edges_input: Arc<dyn ExecutionPlan>,
    pub provider: Arc<dyn LlmProvider>,
    cache: PlanProperties,
}

impl fmt::Debug for InformationGapExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InformationGapExec")
    }
}

impl InformationGapExec {
    pub fn new(
        nodes_input: Arc<dyn ExecutionPlan>,
        edges_input: Arc<dyn ExecutionPlan>,
        provider: Arc<dyn LlmProvider>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("query", DataType::Utf8, true)]));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            nodes_input,
            edges_input,
            provider,
            cache,
        }
    }
}

impl DisplayAs for InformationGapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InformationGapExec")
    }
}

impl ExecutionPlan for InformationGapExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("query", DataType::Utf8, true)]))
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.nodes_input, &self.edges_input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.provider.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let nodes = self.nodes_input.execute(partition, context.clone())?;
        let edges = self.edges_input.execute(partition, context)?;
        Ok(Box::pin(InformationGapStream {
            nodes,
            edges,
            provider: self.provider.clone(),
            state: GapStreamState::Polling,
            collected_nodes: Vec::new(),
            collected_edges: Vec::new(),
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "InformationGapExec"
    }
}

enum GapStreamState {
    Polling,
    Analyzing(BoxFuture<'static, std::result::Result<RecordBatch, DataFusionError>>),
    Done,
}

struct InformationGapStream {
    nodes: SendableRecordBatchStream,
    edges: SendableRecordBatchStream,
    provider: Arc<dyn LlmProvider>,
    state: GapStreamState,
    collected_nodes: Vec<RecordBatch>,
    collected_edges: Vec<RecordBatch>,
}

impl Stream for InformationGapStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                GapStreamState::Polling => {
                    let mut all_done = true;
                    match self.nodes.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.collected_nodes.push(batch);
                            all_done = false;
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        _ => {}
                    }
                    match self.edges.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.collected_edges.push(batch);
                            all_done = false;
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        _ => {}
                    }

                    if all_done {
                        let provider = self.provider.clone();
                        let nodes = std::mem::take(&mut self.collected_nodes);
                        let edges = std::mem::take(&mut self.collected_edges);

                        let fut = async move {
                            let mut graph_summary = String::new();
                            for batch in nodes {
                                let labels = batch
                                    .column(1)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                for i in 0..batch.num_rows() {
                                    graph_summary.push_str(&format!("Node: {}\n", labels.value(i)));
                                }
                            }
                            for batch in edges {
                                let types = batch
                                    .column(2)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                for i in 0..batch.num_rows() {
                                    graph_summary
                                        .push_str(&format!("Edge Type: {}\n", types.value(i)));
                                }
                            }

                            let prompt = format!(
                                "{}\nGraph Summary:\n{}",
                                GAP_ANALYSIS_PROMPT, graph_summary
                            );
                            if let Ok(response) = provider.complete(&prompt).await {
                                let json_str =
                                    crate::nodes::ai::ConfigWizard::extract_json_from_response(
                                        &response,
                                    )
                                    .unwrap_or(&response);
                                if let Ok(json) = serde_json::from_str::<JsonValue>(json_str) {
                                    if let Some(queries) = json["suggested_queries"].as_array() {
                                        let mut builder = StringBuilder::new();
                                        for q in queries {
                                            builder.append_value(q.as_str().unwrap_or(""));
                                        }
                                        let batch = RecordBatch::try_new(
                                            Arc::new(Schema::new(vec![Field::new(
                                                "query",
                                                DataType::Utf8,
                                                true,
                                            )])),
                                            vec![Arc::new(builder.finish())],
                                        )
                                        .map_err(|e| {
                                            DataFusionError::ArrowError(Box::new(e), None)
                                        })?;
                                        return Ok(batch);
                                    }
                                }
                            }
                            // Fallback to empty batch
                            Ok(RecordBatch::new_empty(Arc::new(Schema::new(vec![
                                Field::new("query", DataType::Utf8, true),
                            ]))))
                        };
                        self.state = GapStreamState::Analyzing(Box::pin(fut));
                    } else {
                        return Poll::Pending;
                    }
                }
                GapStreamState::Analyzing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.state = GapStreamState::Done;
                        return Poll::Ready(Some(result));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                GapStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for InformationGapStream {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("query", DataType::Utf8, true)]))
    }
}

pub struct ResearchLoopMetaNode {
    pub state: Arc<Mutex<ResearchState>>,
}

impl ResearchLoopMetaNode {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ResearchState::new(2))),
        }
    }
}

impl Default for ResearchLoopMetaNode {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeBehavior for ResearchLoopMetaNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.research_loop".to_string(),
            display_name: "Deep Research Loop".to_string(),
            description: "Orchestrate recursive web research, extraction, and graph indexing."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "query".into(),
                description: "Initial research topic".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes".into(),
                    description: "Extracted Entities".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges".into(),
                    description: "Extracted Relationships".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_depth": { "type": "integer", "default": 2 },
                    "max_pages_per_step": { "type": "integer", "default": 5 }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let max_depth = config["max_depth"].as_u64().unwrap_or(2) as usize;
        let max_pages = config["max_pages_per_step"].as_u64().unwrap_or(5) as usize;

        if inputs.is_empty() {
            return Err(anyhow!("ResearchLoop requires an initial query input"));
        }

        Ok(vec![
            LogicalPlan::Extension(Extension {
                node: Arc::new(ResearchLoopLogical {
                    input: inputs[0].clone(),
                    max_depth,
                    max_pages,
                    state: self.state.clone(),
                    port: 0,
                }),
            }),
            LogicalPlan::Extension(Extension {
                node: Arc::new(ResearchLoopLogical {
                    input: inputs[0].clone(),
                    max_depth,
                    max_pages,
                    state: self.state.clone(),
                    port: 1,
                }),
            }),
        ])
    }
}

#[derive(Debug, Clone)]
pub struct ResearchLoopLogical {
    pub input: LogicalPlan,
    pub max_depth: usize,
    pub max_pages: usize,
    pub state: Arc<Mutex<ResearchState>>,
    pub port: usize,
}

impl PartialEq for ResearchLoopLogical {
    fn eq(&self, other: &Self) -> bool {
        self.max_depth == other.max_depth
            && self.max_pages == other.max_pages
            && self.port == other.port
    }
}

impl Eq for ResearchLoopLogical {}

impl std::hash::Hash for ResearchLoopLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.max_depth.hash(state);
        self.max_pages.hash(state);
        self.port.hash(state);
    }
}

impl PartialOrd for ResearchLoopLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.port.partial_cmp(&other.port)
    }
}

impl UserDefinedLogicalNodeCore for ResearchLoopLogical {
    fn name(&self) -> &str {
        "ResearchLoop"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref NODE_DF_SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(GraphSchemas::node_schema().as_ref().clone()).unwrap());
            static ref EDGE_DF_SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(GraphSchemas::edge_schema().as_ref().clone()).unwrap());
        }
        if self.port == 0 {
            &NODE_DF_SCHEMA
        } else {
            &EDGE_DF_SCHEMA
        }
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ResearchLoop(port={})", self.port)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            input: inputs[0].clone(),
            ..self.clone()
        })
    }
}

pub struct ResearchLoopExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub max_depth: usize,
    pub max_pages: usize,
    pub state: Arc<Mutex<ResearchState>>,
    pub port: usize,
    pub cache_dir: String,
    cache: PlanProperties,
}

impl fmt::Debug for ResearchLoopExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResearchLoopExec(port={})", self.port)
    }
}

impl ResearchLoopExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        max_depth: usize,
        max_pages: usize,
        state: Arc<Mutex<ResearchState>>,
        port: usize,
        cache_dir: String,
    ) -> Self {
        let schema = if port == 0 {
            GraphSchemas::node_schema()
        } else {
            GraphSchemas::edge_schema()
        };
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            max_depth,
            max_pages,
            state,
            port,
            cache_dir,
            cache,
        }
    }
}

impl DisplayAs for ResearchLoopExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ResearchLoopExec(port={})", self.port)
    }
}

impl ExecutionPlan for ResearchLoopExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        if self.port == 0 {
            GraphSchemas::node_schema()
        } else {
            GraphSchemas::edge_schema()
        }
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
            self.max_depth,
            self.max_pages,
            self.state.clone(),
            self.port,
            self.cache_dir.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(ResearchLoopStream {
            input,
            max_depth: self.max_depth,
            max_pages: self.max_pages,
            state: self.state.clone(),
            port: self.port,
            cache_dir: self.cache_dir.clone(),
            stream_state: LoopStreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "ResearchLoopExec"
    }
}

enum LoopStreamState {
    PollingInput,
    RunningLoop(BoxFuture<'static, std::result::Result<RecordBatch, DataFusionError>>),
    Done,
}

struct ResearchLoopStream {
    input: SendableRecordBatchStream,
    max_depth: usize,
    max_pages: usize,
    state: Arc<Mutex<ResearchState>>,
    port: usize,
    cache_dir: String,
    stream_state: LoopStreamState,
}

impl Stream for ResearchLoopStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.stream_state {
                LoopStreamState::PollingInput => {
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(_batch))) => {
                            let _state = self.state.clone();
                            let port = self.port;
                            let max_depth = self.max_depth;
                            let max_pages = self.max_pages;
                            let cache_dir = self.cache_dir.clone();
                            // For MVP, we simulate the loop results by passing through
                            // In a full implementation, this fut would orchestrate McpInvoke -> EntityExtractor -> InformationGap -> Repeat
                            let fut = async move {
                                // Simulate research cycle
                                println!(
                                    "DEBUG: [ResearchLoop] Starting recursive research at port {} (max_depth={}, max_pages={}, cache_dir={})",
                                    port, max_depth, max_pages, cache_dir
                                );

                                // We MUST return a batch that matches the Golden Schema for this port
                                if port == 0 {
                                    Ok(RecordBatch::new_empty(GraphSchemas::node_schema()))
                                } else {
                                    Ok(RecordBatch::new_empty(GraphSchemas::edge_schema()))
                                }
                            };
                            self.stream_state = LoopStreamState::RunningLoop(Box::pin(fut));
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.stream_state = LoopStreamState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                LoopStreamState::RunningLoop(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        self.stream_state = LoopStreamState::PollingInput;
                        return Poll::Ready(Some(result));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                LoopStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for ResearchLoopStream {
    fn schema(&self) -> SchemaRef {
        if self.port == 0 {
            GraphSchemas::node_schema()
        } else {
            GraphSchemas::edge_schema()
        }
    }
}

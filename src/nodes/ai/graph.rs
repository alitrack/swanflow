use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Int64Builder, StringBuilder, Float64Builder, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore, Extension};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::fmt;
use datafusion::arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use datafusion::physical_plan::{ExecutionPlan, PlanProperties, DisplayAs, DisplayFormatType, RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::execution::context::TaskContext;
use datafusion::error::DataFusionError;
use datafusion::prelude::{SessionContext, DataFrame};
use futures::Stream;
use futures::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::future::BoxFuture;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, GraphSchemas};
use crate::nodes::ai::providers::LlmProvider;
use crate::nodes::common::hash_string_to_i64;

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
            entity_types: config["entity_types"].as_array()
                .map(|a| a.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect())
                .unwrap_or_default(),
            relation_types: config["relation_types"].as_array()
                .map(|a| a.iter().map(|v| v.as_str().unwrap_or("").to_string()).collect())
                .unwrap_or_default(),
            provider: self.provider.clone(),
        });

        Ok(vec![
            LogicalPlan::Extension(Extension { node: Arc::new(EntityPortLogical { base: node.clone(), port: 0 }) }),
            LogicalPlan::Extension(Extension { node: Arc::new(EntityPortLogical { base: node.clone(), port: 1 }) }),
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
        self.port == other.port && self.base.entity_types == other.base.entity_types && self.base.relation_types == other.base.relation_types
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
    fn name(&self) -> &str { "EntityPort" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.base.input] }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref NODE_DF_SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(GraphSchemas::node_schema().as_ref().clone()).unwrap());
            static ref EDGE_DF_SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(GraphSchemas::edge_schema().as_ref().clone()).unwrap());
        }
        if self.port == 0 { &NODE_DF_SCHEMA } else { &EDGE_DF_SCHEMA }
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "EntityPort(port={})", self.port) }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, inputs: Vec<LogicalPlan>) -> std::result::Result<Self, DataFusionError> {
        let mut base = self.base.as_ref().clone();
        base.input = inputs[0].clone();
        Ok(Self { base: Arc::new(base), port: self.port })
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
    pub fn new(input: Arc<dyn ExecutionPlan>, entity_types: Vec<String>, relation_types: Vec<String>, provider: Arc<dyn LlmProvider>, port: usize) -> Self {
        let schema = if port == 0 { GraphSchemas::node_schema() } else { GraphSchemas::edge_schema() };
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self { input, entity_types, relation_types, provider, port, cache }
    }
}

impl DisplayAs for EntityExtractorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EntityExtractorExec(port={})", self.port)
    }
}

impl ExecutionPlan for EntityExtractorExec {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn schema(&self) -> SchemaRef { if self.port == 0 { GraphSchemas::node_schema() } else { GraphSchemas::edge_schema() } }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(children[0].clone(), self.entity_types.clone(), self.relation_types.clone(), self.provider.clone(), self.port)))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(EntityExtractionStream {
            input,
            provider: self.provider.clone(),
            port: self.port,
            state: StreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn name(&self) -> &str { "EntityExtractorExec" }
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
    state: StreamState,
}

impl Stream for EntityExtractionStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::PollingInput => {
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let provider = self.provider.clone();
                            let port = self.port;
                            
                            let fut = async move {
                                let mut node_id_builder = Int64Builder::new();
                                let mut node_label_builder = StringBuilder::new();
                                let mut node_props_builder = StringBuilder::new();

                                let mut edge_src_builder = Int64Builder::new();
                                let mut edge_dst_builder = Int64Builder::new();
                                let mut edge_type_builder = StringBuilder::new();
                                let mut edge_weight_builder = Float64Builder::new();

                                let text_col = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
                                
                                for i in 0..batch.num_rows() {
                                    let text = text_col.value(i);
                                    let prompt = format!(
                                        "Extract entities and relationships from this text.
                                        Return JSON format:
                                        {{
                                          \"nodes\": [{{ \"name\": \"...\", \"label\": \"...\", \"properties\": {{}} }}],
                                          \"edges\": [{{ \"source\": \"...\", \"target\": \"...\", \"type\": \"...\", \"weight\": 1.0 }}]
                                        }}
                                        Text: {}", 
                                        text
                                    );

                                    if let Ok(result) = provider.complete(&prompt).await {
                                        let json_str = crate::nodes::ai::ConfigWizard::extract_json_from_response(&result).unwrap_or(&result);
                                        if let Ok(json) = serde_json::from_str::<JsonValue>(json_str) {
                                            if let Some(nodes) = json["nodes"].as_array() {
                                                for n in nodes {
                                                    let name = n["name"].as_str().unwrap_or("");
                                                    node_id_builder.append_value(hash_string_to_i64(name));
                                                    node_label_builder.append_value(n["label"].as_str().unwrap_or(""));
                                                    node_props_builder.append_value(n["properties"].to_string());
                                                }
                                            }
                                            if let Some(edges) = json["edges"].as_array() {
                                                for e in edges {
                                                    let src = e["source"].as_str().unwrap_or("");
                                                    let dst = e["target"].as_str().unwrap_or("");
                                                    edge_src_builder.append_value(hash_string_to_i64(src));
                                                    edge_dst_builder.append_value(hash_string_to_i64(dst));
                                                    edge_type_builder.append_value(e["type"].as_str().unwrap_or(""));
                                                    edge_weight_builder.append_value(e["weight"].as_f64().unwrap_or(1.0));
                                                }
                                            }
                                        }
                                    }
                                }

                                if port == 0 {
                                    let batch = RecordBatch::try_new(GraphSchemas::node_schema(), vec![
                                        Arc::new(node_id_builder.finish()),
                                        Arc::new(node_label_builder.finish()),
                                        Arc::new(node_props_builder.finish()),
                                    ]).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                    Ok(batch)
                                } else {
                                    let batch = RecordBatch::try_new(GraphSchemas::edge_schema(), vec![
                                        Arc::new(edge_src_builder.finish()),
                                        Arc::new(edge_dst_builder.finish()),
                                        Arc::new(edge_type_builder.finish()),
                                        Arc::new(edge_weight_builder.finish()),
                                    ]).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
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
                    }
                }
                StreamState::Processing(fut) => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            self.state = StreamState::PollingInput;
                            return Poll::Ready(Some(result));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for EntityExtractionStream {
    fn schema(&self) -> SchemaRef { if self.port == 0 { GraphSchemas::node_schema() } else { GraphSchemas::edge_schema() } }
}

pub struct GraphRetrieverNode;

#[async_trait]
impl NodeBehavior for GraphRetrieverNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "ai.graph_retriever".to_string(),
            display_name: "Graph Retriever".to_string(),
            description: "Retrieve multi-hop neighborhood from graph index".to_string(),
            input_ports: vec![
                PortSpec { id: 0, name: "entities".into(), description: "Entities to search from".into() },
            ],
            output_ports: vec![
                PortSpec { id: 0, name: "context".into(), description: "Textual context from graph".into() },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "depth": { "type": "integer", "default": 1 }
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

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(GraphRetrieverLogical {
                input: inputs[0].clone(),
                depth,
            }),
        })])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct GraphRetrieverLogical {
    pub input: LogicalPlan,
    pub depth: u64,
}

impl UserDefinedLogicalNodeCore for GraphRetrieverLogical {
    fn name(&self) -> &str { "GraphRetriever" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref SCHEMA: DFSchemaRef = Arc::new(datafusion::common::DFSchema::try_from(Schema::new(vec![
                Field::new("context", DataType::Utf8, true),
            ])).unwrap());
        }
        &SCHEMA
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "GraphRetriever(depth={})", self.depth) }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, inputs: Vec<LogicalPlan>) -> std::result::Result<Self, DataFusionError> {
        Ok(Self { input: inputs[0].clone(), depth: self.depth })
    }
}

pub struct GraphRetrieverExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub depth: u64,
    pub cache_dir: String,
    cache: PlanProperties,
}

impl fmt::Debug for GraphRetrieverExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphRetrieverExec")
            .field("depth", &self.depth)
            .finish()
    }
}

impl GraphRetrieverExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, depth: u64, cache_dir: String) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("context", DataType::Utf8, true)]));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self { input, depth, cache_dir, cache }
    }
}

impl DisplayAs for GraphRetrieverExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphRetrieverExec(depth={})", self.depth)
    }
}

impl ExecutionPlan for GraphRetrieverExec {
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn schema(&self) -> SchemaRef { Arc::new(Schema::new(vec![Field::new("context", DataType::Utf8, true)])) }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(children[0].clone(), self.depth, self.cache_dir.clone())))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(GraphRetrievalStream {
            input,
            cache_dir: self.cache_dir.clone(),
            state: StreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn name(&self) -> &str { "GraphRetrieverExec" }
}

struct GraphRetrievalStream {
    input: SendableRecordBatchStream,
    cache_dir: String,
    state: StreamState,
}

impl Stream for GraphRetrievalStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::PollingInput => {
                    match self.input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            let cache_dir = self.cache_dir.clone();
                            let fut = async move {
                                let mut context_builder = StringBuilder::new();
                                let entity_ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                                let metadata_path = format!("{}/metadata.duckdb", cache_dir);
                                
                                for i in 0..batch.num_rows() {
                                    let eid = entity_ids.value(i);
                                    let query = format!(
                                        "SELECT label, properties FROM __graph_nodes WHERE id = {}
                                         UNION ALL
                                         SELECT n.label, n.properties 
                                         FROM __graph_edges e 
                                         JOIN __graph_nodes n ON e.target_id = n.id 
                                         WHERE e.source_id = {}", 
                                        eid, eid
                                    );

                                    if let Ok(batches) = crate::nodes::io::DuckDBHelper::query_arrow(":memory:", &query, &vec![], &Some(format!("ATTACH '{}' AS meta (TYPE duckdb);", metadata_path))).await {
                                        for b in batches {
                                            let labels = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                                            for j in 0..b.num_rows() {
                                                context_builder.append_value(format!("Entity: {}", labels.value(j)));
                                            }
                                        }
                                    }
                                }

                                let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("context", DataType::Utf8, true)])), vec![
                                    Arc::new(context_builder.finish()),
                                ]).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                                Ok(batch)
                            };
                            self.state = StreamState::Processing(Box::pin(fut));
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                        Poll::Ready(None) => {
                            self.state = StreamState::Done;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                StreamState::Processing(fut) => {
                    match fut.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            self.state = StreamState::PollingInput;
                            return Poll::Ready(Some(result));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for GraphRetrievalStream {
    fn schema(&self) -> SchemaRef { Arc::new(Schema::new(vec![Field::new("context", DataType::Utf8, true)])) }
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
                PortSpec { id: 0, name: "vector".into(), description: "Vector search results".into() },
                PortSpec { id: 1, name: "graph".into(), description: "Graph retrieval results".into() },
            ],
            output_ports: vec![
                PortSpec { id: 0, name: "context".into(), description: "Final fused context".into() },
            ],
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
            return Err(anyhow!("ContextFuser requires at least two context sources"));
        }

        let ctx = SessionContext::new();
        
        let v_df = DataFrame::new(ctx.state(), inputs[0].clone());
        let g_df = DataFrame::new(ctx.state(), inputs[1].clone());
        
        ctx.register_table("v_input", v_df.into_view())?;
        ctx.register_table("g_input", g_df.into_view())?;
        
        let fused = ctx.sql("SELECT context FROM v_input UNION SELECT context FROM g_input").await?;

        Ok(vec![fused.into_optimized_plan()?])
    }
}

pub struct ResearchLoopMetaNode;

#[async_trait]
impl NodeBehavior for ResearchLoopMetaNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.research_loop".to_string(),
            display_name: "Deep Research Loop".to_string(),
            description: "Orchestrate recursive web research, extraction, and graph indexing.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "query".into(),
                description: "Initial research topic".into(),
            }],
            output_ports: vec![
                PortSpec { id: 0, name: "nodes".into(), description: "Extracted Entities".into() },
                PortSpec { id: 1, name: "edges".into(), description: "Extracted Relationships".into() },
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
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        // ResearchLoop is a MetaNode. It returns a plan that combines
        // its internal ingestion and extraction nodes.
        // For the MVP, we return empty plans matching GraphSchemas.
        use crate::nodes::GraphSchemas;
        
        let node_plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: datafusion::common::DFSchemaRef::new(
                datafusion::common::DFSchema::try_from(GraphSchemas::node_schema().as_ref().clone())?
            ),
        });

        let edge_plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: datafusion::common::DFSchemaRef::new(
                datafusion::common::DFSchema::try_from(GraphSchemas::edge_schema().as_ref().clone())?
            ),
        });

        Ok(vec![node_plan, edge_plan])
    }
}

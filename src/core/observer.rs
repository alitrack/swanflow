use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::nodes::{NodeEvent, NodeState};

/// A custom logical node that represents an observation point in the graph.
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd)]
pub struct StateProbeLogical {
    pub node_id: Uuid,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for StateProbeLogical {
    fn name(&self) -> &str {
        "StateProbe"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StateProbe(node_id={})", self.node_id)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            node_id: self.node_id,
            input: inputs[0].clone(),
        })
    }
}

/// A physical execution plan that wraps another plan and emits state events.
#[derive(Debug)]
pub struct StateProbeExec {
    pub node_id: Uuid,
    pub input: Arc<dyn ExecutionPlan>,
    pub event_tx: mpsc::UnboundedSender<NodeEvent>,
    cache: PlanProperties,
}

impl StateProbeExec {
    pub fn new(
        node_id: Uuid,
        input: Arc<dyn ExecutionPlan>,
        event_tx: mpsc::UnboundedSender<NodeEvent>,
    ) -> Self {
        let input_props = input.properties();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input_props.output_partitioning().clone(),
            input_props.emission_type,
            input_props.boundedness,
        );
        Self {
            node_id,
            input,
            event_tx,
            cache,
        }
    }
}

impl DisplayAs for StateProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StateProbeExec(node_id={})", self.node_id)
    }
}

impl ExecutionPlan for StateProbeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "StateProbeExec"
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.node_id,
            children[0].clone(),
            self.event_tx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(ObservedStream {
            node_id: self.node_id,
            input: input_stream,
            event_tx: self.event_tx.clone(),
            has_started: false,
        }))
    }
}

/// A stream wrapper that emits events when polling starts and ends.
struct ObservedStream {
    node_id: Uuid,
    input: SendableRecordBatchStream,
    event_tx: mpsc::UnboundedSender<NodeEvent>,
    has_started: bool,
}

impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

impl Stream for ObservedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.has_started {
            let _ = self.event_tx.send(NodeEvent::StateChanged {
                node_id: self.node_id,
                state: NodeState::Running,
            });
            self.has_started = true;
        }

        match self.input.poll_next_unpin(cx) {
            Poll::Ready(None) => {
                let _ = self.event_tx.send(NodeEvent::StateChanged {
                    node_id: self.node_id,
                    state: NodeState::Executed,
                });
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                let _ = self.event_tx.send(NodeEvent::StateChanged {
                    node_id: self.node_id,
                    state: NodeState::Error(e.to_string()),
                });
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

/// A custom logical node that represents a materialization point.
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd)]
pub struct MaterializerLogical {
    pub node_id: Uuid,
    pub table_name: String,
    pub cache_key: String,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for MaterializerLogical {
    fn name(&self) -> &str {
        "Materializer"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Materializer(table={}, cache_key={})", self.table_name, self.cache_key)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            node_id: self.node_id,
            table_name: self.table_name.clone(),
            cache_key: self.cache_key.clone(),
            input: inputs[0].clone(),
        })
    }
}

use datafusion::execution::context::SessionContext;

/// A physical execution plan that writes data to DuckLake while passing it through.
pub struct MaterializerExec {
    pub node_id: Uuid,
    pub table_name: String,
    pub cache_key: String,
    pub input: Arc<dyn ExecutionPlan>,
    pub ctx: SessionContext,
    cache: PlanProperties,
}

impl std::fmt::Debug for MaterializerExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializerExec")
            .field("node_id", &self.node_id)
            .field("table_name", &self.table_name)
            .field("cache_key", &self.cache_key)
            .finish()
    }
}

impl MaterializerExec {
    pub fn new(node_id: Uuid, table_name: String, cache_key: String, input: Arc<dyn ExecutionPlan>, ctx: SessionContext) -> Self {
        let input_props = input.properties();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input_props.output_partitioning().clone(),
            input_props.emission_type,
            input_props.boundedness,
        );
        Self {
            node_id,
            table_name,
            cache_key,
            input,
            ctx,
            cache,
        }
    }
}

impl DisplayAs for MaterializerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MaterializerExec(table={}, cache_key={})", self.table_name, self.cache_key)
    }
}

impl ExecutionPlan for MaterializerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "MaterializerExec"
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.node_id,
            self.table_name.clone(),
            self.cache_key.clone(),
            children[0].clone(),
            self.ctx.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(MaterializingStream {
            node_id: self.node_id,
            table_name: self.table_name.clone(),
            cache_key: self.cache_key.clone(),
            input: input_stream,
            batches: Vec::new(),
            ctx: self.ctx.clone(),
        }))
    }
}

struct MaterializingStream {
    node_id: Uuid,
    table_name: String,
    cache_key: String,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    ctx: SessionContext,
}

impl RecordBatchStream for MaterializingStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

impl Stream for MaterializingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.batches.push(batch.clone());
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                // End of stream, write to file if we have batches
                if !self.batches.is_empty() {
                    let batches = std::mem::take(&mut self.batches);
                    
                    let table_name = self.table_name.clone();
                    let cache_dir = "target/swanflow_cache";
                    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
                    
                    let schema = batches[0].schema();
                    let path = format!("{}/{}.parquet", cache_dir, self.cache_key);
                    
                    // 1. Write the Parquet file
                    if let Ok(f) = std::fs::File::create(&path) {
                        let mut writer = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                        for batch in &batches {
                            writer.write(batch).unwrap();
                        }
                        writer.close().unwrap();
                        
                                // 2. Commit to Metadata DB directly
                                if let Ok(conn) = duckdb::Connection::open(&metadata_path) {
                                    conn.execute(
                                        "INSERT OR REPLACE INTO flow_metadata (node_id, table_name, parquet_path, schema_json) 
                                         VALUES (?1, ?2, ?3, ?4)",
                                        [
                                            self.node_id.to_string(),
                                            table_name.clone(),
                                            path.clone(),
                                            serde_json::to_string(&schema.to_string()).unwrap_or_default(),
                                        ],
                                    ).ok();
                                    
                                    // Synchronous-like registration by spawning and waiting if needed
                                    // For now, let's use a simpler approach: the engine will handle registration 
                                    // but we ensure the file is closed and metadata is committed first.
                                    tracing::info!("Materialized node {} to table {} via DuckDB index", self.node_id, table_name);
                                }
                    }
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

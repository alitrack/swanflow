use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::{TaskContext, SessionContext};
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

use crate::nodes::NodeEvent;

#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd)]
pub struct StateProbeLogical {
    pub node_id: Uuid,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for StateProbeLogical {
    fn name(&self) -> &str { "StateProbe" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }
    fn schema(&self) -> &DFSchemaRef { self.input.schema() }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "StateProbe(node_id={})", self.node_id) }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self { node_id: self.node_id, input: inputs[0].clone() })
    }
}

#[derive(Debug)]
pub struct StateProbeExec {
    pub node_id: Uuid,
    pub input: Arc<dyn ExecutionPlan>,
    pub event_tx: mpsc::UnboundedSender<NodeEvent>,
    cache: PlanProperties,
}

impl StateProbeExec {
    pub fn new(node_id: Uuid, input: Arc<dyn ExecutionPlan>, event_tx: mpsc::UnboundedSender<NodeEvent>) -> Self {
        let input_props = input.properties();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input_props.output_partitioning().clone(),
            input_props.emission_type,
            input_props.boundedness,
        );
        Self { node_id, input, event_tx, cache }
    }
}

impl DisplayAs for StateProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "StateProbeExec(node_id={})", self.node_id) }
}

impl ExecutionPlan for StateProbeExec {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "StateProbeExec" }
    fn schema(&self) -> SchemaRef { self.input.schema() }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(self.node_id, children[0].clone(), self.event_tx.clone())))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(StateProbeStream { node_id: self.node_id, input: input_stream, event_tx: self.event_tx.clone() }))
    }
}

struct StateProbeStream {
    node_id: Uuid,
    input: SendableRecordBatchStream,
    event_tx: mpsc::UnboundedSender<NodeEvent>,
}

impl Stream for StateProbeStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let _ = self.event_tx.send(NodeEvent::StateChanged { 
                    node_id: self.node_id, 
                    state: crate::nodes::NodeState::Running
                });
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for StateProbeStream {
    fn schema(&self) -> SchemaRef { self.input.schema() }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd)]
pub struct MaterializerLogical {
    pub node_id: Uuid,
    pub table_name: String,
    pub cache_key: String,
    pub cache_dir: String,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for MaterializerLogical {
    fn name(&self) -> &str { "Materializer" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }
    fn schema(&self) -> &DFSchemaRef { self.input.schema() }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "Materializer(table={}, cache_dir={})", self.table_name, self.cache_dir) }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self { 
            node_id: self.node_id, 
            table_name: self.table_name.clone(), 
            cache_key: self.cache_key.clone(), 
            cache_dir: self.cache_dir.clone(),
            input: inputs[0].clone() 
        })
    }
}

pub struct MaterializerExec {
    pub node_id: Uuid,
    pub table_name: String,
    pub cache_key: String,
    pub cache_dir: String,
    pub input: Arc<dyn ExecutionPlan>,
    pub ctx: SessionContext,
    cache: PlanProperties,
}

impl std::fmt::Debug for MaterializerExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializerExec")
            .field("table", &self.table_name)
            .field("cache_dir", &self.cache_dir)
            .finish()
    }
}

impl MaterializerExec {
    pub fn new(node_id: Uuid, table_name: String, cache_key: String, cache_dir: String, input: Arc<dyn ExecutionPlan>, ctx: SessionContext) -> Self {
        let input_props = input.properties();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            input_props.emission_type,
            input_props.boundedness,
        );
        Self { node_id, table_name, cache_key, cache_dir, input, ctx, cache }
    }
}

impl DisplayAs for MaterializerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result { 
        write!(f, "MaterializerExec(table={}, cache_dir={})", self.table_name, self.cache_dir) 
    }
}

impl ExecutionPlan for MaterializerExec {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "MaterializerExec" }
    fn schema(&self) -> SchemaRef { self.input.schema() }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.node_id, 
            self.table_name.clone(), 
            self.cache_key.clone(), 
            self.cache_dir.clone(),
            children[0].clone(), 
            self.ctx.clone()
        )))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(MaterializingStream {
            node_id: self.node_id,
            table_name: self.table_name.clone(),
            cache_key: self.cache_key.clone(),
            cache_dir: self.cache_dir.clone(),
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
    cache_dir: String,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    ctx: SessionContext,
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
                if !self.batches.is_empty() {
                    let num_rows: usize = self.batches.iter().map(|b| b.num_rows()).sum();
                    
                    let batches = std::mem::take(&mut self.batches);
                    let table_name = self.table_name.clone();
                    let cache_dir = self.cache_dir.clone();
                    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
                    let schema = batches[0].schema();
                    let path = format!("{}/{}.parquet", cache_dir, self.cache_key);

                    if let Ok(f) = std::fs::File::create(&path) {
                        let mut writer = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                        for batch in &batches { writer.write(batch).unwrap(); }
                        writer.close().unwrap();

                        if let Ok(conn) = duckdb::Connection::open(&metadata_path) {
                            conn.execute(
                                "INSERT OR REPLACE INTO flow_metadata (node_id, table_name, parquet_path, schema_json) VALUES (?1, ?2, ?3, ?4)",
                                [self.node_id.to_string(), table_name.clone(), path.clone(), serde_json::to_string(&schema.to_string()).unwrap_or_default()],
                            ).ok();
                            let ctx = self.ctx.clone();
                            let table_name_clone = table_name.clone();
                            tokio::spawn(async move {
                                tracing::debug!("Live registering table {}", table_name_clone);
                                ctx.register_parquet(&table_name_clone, &path, datafusion::prelude::ParquetReadOptions::default()).await.ok();
                            });
                        }
                    }
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl RecordBatchStream for MaterializingStream {
    fn schema(&self) -> SchemaRef { self.input.schema() }
}

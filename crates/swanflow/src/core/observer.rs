use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, OrderingRequirements};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::nodes::NodeEvent;

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
        write!(f, "StateProbe(id={})", self.node_id)
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
        // CRITICAL: coalesce input partitions to 1. Without this, a multi-partition
        // input (e.g. UnionExec with one partition per UNION ALL branch) silently
        // drops all partitions except 0 — only partition 0's rows ever flow
        // downstream and get materialized.
        // Bug 17: when the input has a declared ordering (e.g. a user ORDER BY
        // planned below this node), a plain CoalescePartitionsExec would merge
        // partitions non-order-preserving and shuffle rows. Use a
        // SortPreservingMerge instead so the order survives the coalesce.
        let input: Arc<dyn ExecutionPlan> = match input.properties().equivalence_properties().output_ordering() {
            Some(ordering) => {
                let lex = datafusion::physical_expr::LexOrdering::new(ordering.iter().cloned())
                    .expect("non-empty output ordering");
                Arc::new(
                    datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(
                        lex, input,
                    ),
                )
            }
            None => Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                    input,
                ),
            ),
        };
        let input_props = input.properties();
        let cache = PlanProperties::new(
            input_props.equivalence_properties().clone(),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
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
        write!(f, "StateProbeExec(id={})", self.node_id)
    }
}

impl ExecutionPlan for StateProbeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.input.schema()
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
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(StateProbeStream {
            node_id: self.node_id,
            input,
            event_tx: self.event_tx.clone(),
            // Set to 500ms ago so first event is sent immediately
            last_event: std::time::Instant::now() - std::time::Duration::from_millis(500),
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    // Bug 17: StateProbe passes rows through untouched — it maintains the
    // input ordering. Without this the physical optimizer sees "no ordering
    // consumer" and strips the user's SortExec below us.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    // Bug 17: soft requirement — if the subtree below us computed an ordering
    // (e.g. a user ORDER BY), ask the optimizer to keep it.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self
            .input
            .properties()
            .equivalence_properties()
            .output_ordering()
            .map(|lex| OrderingRequirements::new_soft(lex.into()))]
    }
    fn name(&self) -> &str {
        "StateProbeExec"
    }
}

struct StateProbeStream {
    node_id: Uuid,
    input: SendableRecordBatchStream,
    event_tx: mpsc::UnboundedSender<NodeEvent>,
    last_event: std::time::Instant,
}

impl Stream for StateProbeStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Throttle StateChanged events to 500ms
                if self.last_event.elapsed() >= std::time::Duration::from_millis(500) {
                    let _ = self.event_tx.send(NodeEvent::StateChanged {
                        node_id: self.node_id,
                        state: crate::nodes::NodeState::Running,
                    });
                    self.last_event = std::time::Instant::now();
                }
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for StateProbeStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

#[derive(Clone)]
pub struct MaterializerLogical {
    pub node_id: Uuid,
    pub table_name: String,
    pub cache_key: String,
    pub cache_dir: String,
    pub input: LogicalPlan,
}

impl std::fmt::Debug for MaterializerLogical {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializerLogical")
            .field("node_id", &self.node_id)
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl std::cmp::PartialEq for MaterializerLogical {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
            && self.table_name == other.table_name
            && self.cache_key == other.cache_key
    }
}

impl std::cmp::Eq for MaterializerLogical {}

impl std::hash::Hash for MaterializerLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        self.table_name.hash(state);
        self.cache_key.hash(state);
    }
}

impl PartialOrd for MaterializerLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
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
        write!(
            f,
            "Materializer(table={}, cache_dir={})",
            self.table_name, self.cache_dir
        )
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
            cache_dir: self.cache_dir.clone(),
            input: inputs[0].clone(),
        })
    }
}

pub struct MaterializerExec {
    pub node_id: Uuid,
    pub table_name: String,
    pub cache_key: String,
    pub cache_dir: String,
    pub input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl std::fmt::Debug for MaterializerExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializerExec")
            .field("node_id", &self.node_id)
            .field("table_name", &self.table_name)
            .finish()
    }
}

impl MaterializerExec {
    pub fn new(
        node_id: Uuid,
        table_name: String,
        cache_key: String,
        cache_dir: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        // Bug 17 (see StateProbeExec::new): keep ordering alive through the
        // coalesce, and propagate the input's equivalence properties (incl.
        // ordering) so the optimizer knows order is preserved by this node.
        let input: Arc<dyn ExecutionPlan> = match input.properties().equivalence_properties().output_ordering() {
            Some(ordering) => {
                let lex = datafusion::physical_expr::LexOrdering::new(ordering.iter().cloned())
                    .expect("non-empty output ordering");
                Arc::new(
                    datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(
                        lex, input,
                    ),
                )
            }
            None => Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                    input,
                ),
            ),
        };
        let input_props = input.properties();
        let cache = PlanProperties::new(
            input_props.equivalence_properties().clone(),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            input_props.emission_type,
            input_props.boundedness,
        );
        Self {
            node_id,
            table_name,
            cache_key,
            cache_dir,
            input,
            cache,
        }
    }
}

impl DisplayAs for MaterializerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MaterializerExec(table={}, cache_dir={})",
            self.table_name, self.cache_dir
        )
    }
}

impl ExecutionPlan for MaterializerExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.input.schema()
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
            self.cache_dir.clone(),
            children[0].clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MaterializingStream {
            node_id: self.node_id,
            table_name: self.table_name.clone(),
            cache_key: self.cache_key.clone(),
            cache_dir: self.cache_dir.clone(),
            input,
            batches: Vec::new(),
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    // Bug 17: see StateProbeExec — materialization writes rows to parquet in
    // stream order, so we must maintain and request the input ordering.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self
            .input
            .properties()
            .equivalence_properties()
            .output_ordering()
            .map(|lex| OrderingRequirements::new_soft(lex.into()))]
    }
    fn name(&self) -> &str {
        "MaterializerExec"
    }
}

struct MaterializingStream {
    node_id: Uuid,
    table_name: String,
    cache_key: String,
    cache_dir: String,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
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
                    let batches = std::mem::take(&mut self.batches);
                    let table_name = self.table_name.clone();
                    let cache_dir = self.cache_dir.clone();
                    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
                    let schema = batches[0].schema();
                    let path = format!("{}/{}.parquet", cache_dir, self.cache_key);
                    let node_id = self.node_id;

                    if let Ok(f) = std::fs::File::create(&path) {
                        let mut writer =
                            parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                        for batch in &batches {
                            writer.write(batch).unwrap();
                        }
                        writer.close().unwrap();

                        if let Ok(conn) = duckdb::Connection::open(&metadata_path) {
                            eprintln!(
                                "DEBUG: Recording materialization for node {} into table {}",
                                node_id, table_name
                            );
                            // ADR-015 rule 6: record the REAL schema
                            // fingerprint (name:type,...) so skip-if-fresh
                            // can prove the parquet still matches this
                            // node's output schema — not just that the
                            // file exists. Pre-hardening rows stored '{}',
                            // which now fails closed exactly once at skip
                            // time (the rerun re-records the fingerprint).
                            let schema_fp = crate::core::engine::assets::schema_fingerprint(schema.as_ref());
                            conn.execute(
                                "INSERT OR REPLACE INTO flow_metadata (node_id, table_name, parquet_path, schema_json) VALUES (?, ?, ?, ?)",
                                [node_id.to_string(), table_name.clone(), path.clone(), schema_fp],
                            ).ok();
                            // WAL discipline (GraphRAG judgment): a later
                            // reader opening a NEW connection on this file
                            // cannot see un-checkpointed WAL content. Skip-if-
                            // fresh readers (try_skip_fresh) open fresh
                            // connections, so checkpoint here or the just-
                            // recorded row is invisible to them.
                            conn.execute("CHECKPOINT", []).ok();
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
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

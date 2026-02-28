use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
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
        // Use the properties method from the ExecutionPlan trait
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
            other => result_to_poll(other),
        }
    }
}

fn result_to_poll<T>(res: Poll<Option<T>>) -> Poll<Option<T>> {
    res
}

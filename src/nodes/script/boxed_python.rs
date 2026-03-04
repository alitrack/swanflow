use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore, Extension};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::any::Any;
use std::fmt;
use uuid::Uuid;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};
use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType, Partitioning, PlanProperties, SendableRecordBatchStream, RecordBatchStream};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::execution::TaskContext;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use futures::{Stream, Future, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::future::BoxFuture;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoxedPythonLogical {
    pub input: LogicalPlan,
    pub code: String,
    pub image: String,
    pub s3_bucket: String,
    pub schema: Arc<DFSchema>,
}

impl PartialOrd for BoxedPythonLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.code.partial_cmp(&other.code)
    }
}

impl UserDefinedLogicalNodeCore for BoxedPythonLogical {
    fn name(&self) -> &str { "BoxedPython" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }
    fn schema(&self) -> &DFSchemaRef { &self.schema }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoxedPython: image={}", self.image)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, inputs: Vec<LogicalPlan>) -> Result<Self, DataFusionError> {
        Ok(Self { input: inputs[0].clone(), ..self.clone() })
    }
}

#[derive(Debug)]
pub struct BoxedPythonExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub code: String,
    pub image: String,
    pub s3_bucket: String,
    pub schema: datafusion::arrow::datatypes::SchemaRef,
    pub cache: PlanProperties,
}

impl BoxedPythonExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        code: String,
        image: String,
        s3_bucket: String,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { input, code, image, s3_bucket, schema, cache }
    }
}

impl DisplayAs for BoxedPythonExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BoxedPythonExec: image={}", self.image)
    }
}

impl ExecutionPlan for BoxedPythonExec {
    fn name(&self) -> &str { "BoxedPythonExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef { self.schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(children[0].clone(), self.code.clone(), self.image.clone(), self.s3_bucket.clone(), self.schema.clone())))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context.clone())?;
        Ok(Box::pin(BoxedPythonStream {
            input,
            code: self.code.clone(),
            image: self.image.clone(),
            s3_bucket: self.s3_bucket.clone(),
            schema: self.schema.clone(),
            fut: None,
            done: false,
        }))
    }
}

pub struct BoxedPythonStream {
    input: SendableRecordBatchStream,
    code: String,
    image: String,
    s3_bucket: String,
    schema: datafusion::arrow::datatypes::SchemaRef,
    fut: Option<Pin<Box<dyn Future<Output = Result<RecordBatch, anyhow::Error>> + Send>>>,
    done: bool,
}

impl Stream for BoxedPythonStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done { return Poll::Ready(None); }

        if self.fut.is_none() {
            let code = self.code.clone();
            let bucket = self.s3_bucket.clone();
            let schema = self.schema.clone();
            let mut input_stream = std::mem::replace(&mut self.input, Box::pin(datafusion::physical_plan::stream::EmptyRecordBatchStream::new(self.schema.clone())));
            
            let fut = async move {
                // 1. Collect input batches
                let mut batches = Vec::new();
                while let Some(batch) = input_stream.next().await {
                    batches.push(batch?);
                }
                
                // 2. Upload to S3 (Simplified for MVP demo)
                println!("DEBUG: [BoxedPython] Uploading {} batches to S3 bucket: {}", batches.len(), bucket);
                
                // 3. Call Sidecar MCP (Placeholder for actual MCP invocation)
                println!("DEBUG: [BoxedPython] Calling BoxLite MCP with image: {}", "python:3.11-slim");
                
                // 4. Return results (Simulate results for now)
                Ok(RecordBatch::new_empty(schema))
            };
            self.fut = Some(Box::pin(fut));
        }

        let res = match Pin::new(self.fut.as_mut().unwrap()).poll(cx) {
            Poll::Ready(res) => res,
            Poll::Pending => return Poll::Pending,
        };

        self.fut = None; 
        self.done = true;

        match res {
            Ok(batch) => Poll::Ready(Some(Ok(batch))),
            Err(e) => Poll::Ready(Some(Err(DataFusionError::Execution(format!("Boxed execution failed: {}", e))))),
        }
    }
}

impl RecordBatchStream for BoxedPythonStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef { self.schema.clone() }
}

pub struct BoxedPythonNode;

#[async_trait]
impl NodeBehavior for BoxedPythonNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.script.boxed_python".to_string(),
            display_name: "Boxed Python".to_string(),
            description: "Execute Python code in a hardware-isolated BoxLite sandbox using S3 for data exchange.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Input Table".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Result Table".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "code": { "type": "string" },
                    "image": { "type": "string", "default": "python:3.11-slim" },
                    "s3_bucket": { "type": "string" }
                },
                "required": ["code", "s3_bucket"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["code"].as_str().ok_or_else(|| anyhow!("code is required"))?;
        config["s3_bucket"].as_str().ok_or_else(|| anyhow!("s3_bucket is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let code = config["code"].as_str().unwrap().to_string();
        let image = config["image"].as_str().unwrap_or("python:3.11-slim").to_string();
        let s3_bucket = config["s3_bucket"].as_str().unwrap().to_string();
        
        let schema = Arc::new(DFSchema::try_from(inputs[0].schema().as_ref().clone().into())?);

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(BoxedPythonLogical {
                input: inputs[0].clone(),
                code,
                image,
                s3_bucket,
                schema,
            }),
        })])
    }
}

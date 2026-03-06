use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use futures::{Future, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};
use datafusion::arrow::array::{BooleanBuilder, StringBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};

// --- MCP Client Logic ---

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: JsonValue,
    pub id: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub result: Option<JsonValue>,
    pub error: Option<JsonValue>,
    pub id: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<JsonValue>,
}

#[async_trait]
pub trait McpTransport: Send + Sync {
    async fn send_message(&self, message: JsonValue) -> Result<()>;
    async fn receive_message(&self) -> Result<JsonValue>;
}

pub struct StdioTransport {
    writer: Mutex<FramedWrite<tokio::process::ChildStdin, LinesCodec>>,
    reader: Mutex<FramedRead<tokio::process::ChildStdout, LinesCodec>>,
    _child: Child,
}

impl StdioTransport {
    pub fn spawn(command: &str, args: &[String]) -> Result<Self> {
        let mut child = Command::new(command)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("Failed to open stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Failed to open stdout"))?;

        let writer = FramedWrite::new(stdin, LinesCodec::new());
        let reader = FramedRead::new(stdout, LinesCodec::new());

        Ok(Self {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            _child: child,
        })
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn send_message(&self, message: JsonValue) -> Result<()> {
        let line = serde_json::to_string(&message)?;
        let mut writer = self.writer.lock().await;
        writer.send(line).await?;
        Ok(())
    }

    async fn receive_message(&self) -> Result<JsonValue> {
        let mut reader = self.reader.lock().await;
        if let Some(line) = reader.next().await {
            let line: String = line?;
            Ok(serde_json::from_str(&line)?)
        } else {
            Err(anyhow!("Stream closed"))
        }
    }
}

pub struct McpClient {
    transport: Box<dyn McpTransport>,
    next_id: i64,
}

impl McpClient {
    pub fn new(transport: Box<dyn McpTransport>) -> Self {
        Self {
            transport,
            next_id: 1,
        }
    }

    pub async fn call(&mut self, method: &str, params: JsonValue) -> Result<JsonValue> {
        let id = self.next_id;
        self.next_id += 1;
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: JsonValue::from(id),
        };
        self.transport
            .send_message(serde_json::to_value(request)?)
            .await?;
        loop {
            let message = self.transport.receive_message().await?;
            if let Ok(response) = serde_json::from_value::<JsonRpcResponse>(message.clone()) {
                if response.id.as_i64() == Some(id) {
                    if let Some(error) = response.error {
                        return Err(anyhow!("MCP Error: {:?}", error));
                    }
                    return Ok(response.result.unwrap_or(JsonValue::Null));
                }
            }
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        let params = serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "SwanFlow", "version": "0.1.0" }
        });
        self.call("initialize", params).await?;
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        self.transport
            .send_message(serde_json::to_value(notification)?)
            .await?;
        Ok(())
    }

    pub async fn call_tool(&mut self, tool_name: &str, arguments: JsonValue) -> Result<JsonValue> {
        let params = serde_json::json!({ "name": tool_name, "arguments": arguments });
        self.call("tools/call", params).await
    }
}

// --- McpInvoke Node Logic ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpInvokeLogical {
    pub tool_name: String,
    pub arguments: String,
    pub server_command: String,
    pub server_args: Vec<String>,
    pub schema: Arc<DFSchema>,
}

impl PartialOrd for McpInvokeLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tool_name.partial_cmp(&other.tool_name)
    }
}

impl UserDefinedLogicalNodeCore for McpInvokeLogical {
    fn name(&self) -> &str {
        "McpInvoke"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "McpInvoke: tool={}, cmd={}",
            self.tool_name, self.server_command
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        Ok(self.clone())
    }
}

#[derive(Debug)]
pub struct McpInvokeExec {
    pub tool_name: String,
    pub arguments: JsonValue,
    pub server_command: String,
    pub server_args: Vec<String>,
    pub schema: datafusion::arrow::datatypes::SchemaRef,
    pub cache: PlanProperties,
}

impl McpInvokeExec {
    pub fn new(
        tool_name: String,
        arguments: JsonValue,
        server_command: String,
        server_args: Vec<String>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            tool_name,
            arguments,
            server_command,
            server_args,
            schema,
            cache,
        }
    }
}

impl DisplayAs for McpInvokeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "McpInvokeExec: tool={}", self.tool_name)
    }
}

impl ExecutionPlan for McpInvokeExec {
    fn name(&self) -> &str {
        "McpInvokeExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Ok(Box::pin(McpInvokeStream {
            tool_name: self.tool_name.clone(),
            arguments: self.arguments.clone(),
            server_command: self.server_command.clone(),
            server_args: self.server_args.clone(),
            schema: self.schema.clone(),
            fut: None,
            done: false,
        }))
    }
}

type McpInvokeFuture = Pin<Box<dyn Future<Output = Result<JsonValue, anyhow::Error>> + Send>>;

pub struct McpInvokeStream {
    tool_name: String,
    arguments: JsonValue,
    server_command: String,
    server_args: Vec<String>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    fut: Option<McpInvokeFuture>,
    done: bool,
}

impl Stream for McpInvokeStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        if self.fut.is_none() {
            let tool_name = self.tool_name.clone();
            let arguments = self.arguments.clone();
            let server_command = self.server_command.clone();
            let server_args = self.server_args.clone();
            let fut = async move {
                let transport = StdioTransport::spawn(&server_command, &server_args)?;
                let mut client = McpClient::new(Box::new(transport));
                client.initialize().await?;
                client.call_tool(&tool_name, arguments).await
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
            Ok(val) => {
                let mut content_builder = StringBuilder::new();
                let mut error_builder = BooleanBuilder::new();
                if let Some(content_array) = val["content"].as_array() {
                    for block in content_array {
                        if let Some(text) = block["text"].as_str() {
                            content_builder.append_value(text);
                            error_builder.append_value(false);
                        } else {
                            content_builder
                                .append_value(serde_json::to_string(block).unwrap_or_default());
                            error_builder.append_value(false);
                        }
                    }
                } else {
                    content_builder.append_value(serde_json::to_string(&val).unwrap_or_default());
                    error_builder.append_value(val["isError"].as_bool().unwrap_or(false));
                }
                let batch = RecordBatch::try_new(
                    self.schema.clone(),
                    vec![
                        Arc::new(content_builder.finish()),
                        Arc::new(error_builder.finish()),
                    ],
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Poll::Ready(Some(Ok(batch)))
            }
            Err(e) => Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                "MCP Invocation Failed: {}",
                e
            ))))),
        }
    }
}

impl RecordBatchStream for McpInvokeStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

pub struct McpInvokeNode;

#[async_trait]
impl NodeBehavior for McpInvokeNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.mcp_invoke".to_string(),
            display_name: "MCP Tool Invoke".to_string(),
            description: "Execute an external AI tool via MCP".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Tool Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "tool_name": { "type": "string" },
                    "arguments": { "type": "object" },
                    "server_command": { "type": "string", "default": "node" },
                    "server_args": { "type": "array", "items": { "type": "string" }, "default": [] }
                },
                "required": ["tool_name"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["tool_name"]
            .as_str()
            .ok_or_else(|| anyhow!("tool_name is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let tool_name = config["tool_name"].as_str().unwrap().to_string();
        let arguments = config["arguments"].clone();
        let server_command = config["server_command"]
            .as_str()
            .unwrap_or("node")
            .to_string();
        let server_args = config["server_args"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();

        let schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "content",
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            ),
            datafusion::arrow::datatypes::Field::new(
                "is_error",
                datafusion::arrow::datatypes::DataType::Boolean,
                true,
            ),
        ]);
        let df_schema = Arc::new(DFSchema::try_from(schema)?);
        let logical_node = McpInvokeLogical {
            tool_name,
            arguments: serde_json::to_string(&arguments).unwrap_or_default(),
            server_command,
            server_args,
            schema: df_schema,
        };
        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(logical_node),
            },
        )])
    }
}

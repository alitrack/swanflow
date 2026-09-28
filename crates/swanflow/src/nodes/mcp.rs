//! McpToolNode — a SwanFlow workflow node that calls external MCP server tools.
//!
//! This node can be placed in a YAML workflow to invoke a tool from an external
//! MCP server as part of a data pipeline.

use crate::mcp::client::McpClient;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{BooleanBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Future, Stream};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

// ---------------------------------------------------------------------------
// MCP Tool Node — Logical Plan Node
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct McpToolLogical {
    pub server_command: String,
    pub server_args: Vec<String>,
    pub tool_name: String,
    pub tool_args: String,
    pub schema: Arc<DFSchema>,
}

impl PartialOrd for McpToolLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tool_name.partial_cmp(&other.tool_name)
    }
}

impl UserDefinedLogicalNodeCore for McpToolLogical {
    fn name(&self) -> &str {
        "McpTool"
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
            "McpTool: tool={}, cmd={} {}",
            self.tool_name,
            self.server_command,
            self.server_args.join(" ")
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

// ---------------------------------------------------------------------------
// MCP Tool Node — Physical Execution Plan
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct McpToolExec {
    pub server_command: String,
    pub server_args: Vec<String>,
    pub tool_name: String,
    pub tool_args: JsonValue,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
}

impl McpToolExec {
    pub fn new(
        server_command: String,
        server_args: Vec<String>,
        tool_name: String,
        tool_args: JsonValue,
        schema: SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            server_command,
            server_args,
            tool_name,
            tool_args,
            schema,
            cache,
        }
    }
}

impl DisplayAs for McpToolExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "McpToolExec: tool={}", self.tool_name)
    }
}

impl ExecutionPlan for McpToolExec {
    fn name(&self) -> &str {
        "McpToolExec"
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
        Ok(Box::pin(McpToolStream {
            server_command: self.server_command.clone(),
            server_args: self.server_args.clone(),
            tool_name: self.tool_name.clone(),
            tool_args: self.tool_args.clone(),
            schema: self.schema.clone(),
            fut: None,
            done: false,
        }))
    }
}

type McpToolFuture = Pin<Box<dyn Future<Output = Result<JsonValue>> + Send>>;

pub struct McpToolStream {
    server_command: String,
    server_args: Vec<String>,
    tool_name: String,
    tool_args: JsonValue,
    schema: SchemaRef,
    fut: Option<McpToolFuture>,
    done: bool,
}


/// Build the standard (content, error) RecordBatch from a tools/call result.
/// Shared by McpToolStream and McpInvokeStream so the two MCP client paths
/// (LSP Content-Length vs NDJSON stdio) cannot drift apart.
pub(crate) fn mcp_result_to_batch(
    val: &JsonValue,
    schema: SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let mut content_builder = StringBuilder::new();
    let mut error_builder = BooleanBuilder::new();
    if let Some(content_array) = val["content"].as_array() {
        for block in content_array {
            if let Some(text) = block["text"].as_str() {
                content_builder.append_value(text);
                error_builder.append_value(false);
            } else {
                content_builder.append_value(serde_json::to_string(block).unwrap_or_default());
                error_builder.append_value(false);
            }
        }
    } else {
        content_builder.append_value(serde_json::to_string(val).unwrap_or_default());
        error_builder.append_value(val["isError"].as_bool().unwrap_or(false));
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(content_builder.finish()),
            Arc::new(error_builder.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

impl Stream for McpToolStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        if self.fut.is_none() {
            let cmd = self.server_command.clone();
            let args = self.server_args.clone();
            let tool = self.tool_name.clone();
            let tool_args = self.tool_args.clone();
            let fut = async move {
                let mut client = McpClient::connect(&cmd, &args).await?;
                let result = client.call_tool(&tool, tool_args).await?;
                client.close().await?;
                Ok(result)
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
                let batch = crate::nodes::mcp::mcp_result_to_batch(&val, self.schema.clone())?;
                Poll::Ready(Some(Ok(batch)))
            }
            Err(e) => Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                "MCP Tool Invocation Failed: {e}"
            ))))),
        }
    }
}

impl RecordBatchStream for McpToolStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// McpToolNode — NodeBehavior implementation
// ---------------------------------------------------------------------------

pub struct McpToolNode;

#[async_trait]
impl NodeBehavior for McpToolNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.mcp_tool".to_string(),
            display_name: "MCP Tool".to_string(),
            description: "Call a tool from an external MCP server within a workflow".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Tool Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "server_command": {
                        "type": "string",
                        "description": "Command to start the MCP server (e.g., 'python', 'node')"
                    },
                    "server_args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Arguments for the MCP server command"
                    },
                    "tool_name": {
                        "type": "string",
                        "description": "Name of the tool to call"
                    },
                    "tool_args": {
                        "type": "object",
                        "description": "Arguments to pass to the tool"
                    }
                },
                "required": ["server_command", "tool_name"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["server_command"]
            .as_str()
            .ok_or_else(|| anyhow!("server_command is required"))?;
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
        let server_command = config["server_command"]
            .as_str()
            .unwrap_or("python")
            .to_string();
        let server_args = config["server_args"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|v| v.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        let tool_name = config["tool_name"].as_str().unwrap_or("").to_string();
        let tool_args = config.get("tool_args").cloned().unwrap_or(JsonValue::Null);

        let schema = Schema::new(vec![
            Field::new("content", DataType::Utf8, true),
            Field::new("is_error", DataType::Boolean, true),
        ]);
        let df_schema = Arc::new(DFSchema::try_from(schema)?);

        let logical_node = McpToolLogical {
            server_command,
            server_args,
            tool_name,
            tool_args: serde_json::to_string(&tool_args).unwrap_or_default(),
            schema: df_schema,
        };

        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(logical_node),
            },
        )])
    }
}

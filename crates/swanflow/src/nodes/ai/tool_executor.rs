use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::{json, Value as JsonValue};

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

pub struct ToolExecutorNode;

impl ToolExecutorNode {
    pub fn normalize_payload(
        &self,
        tool_name: &str,
        raw_payload: &JsonValue,
        latency_ms: u64,
    ) -> Result<JsonValue> {
        if tool_name.trim().is_empty() {
            return Err(anyhow!("tool_name must not be empty"));
        }

        if let Some(err) = raw_payload.get("error") {
            let error = if let Some(msg) = err.as_str() {
                JsonValue::String(msg.to_string())
            } else {
                err.clone()
            };
            return Ok(json!({
                "status": "error",
                "tool_name": tool_name,
                "data": JsonValue::Null,
                "error": error,
                "latency_ms": latency_ms,
            }));
        }

        let data = raw_payload
            .get("result")
            .cloned()
            .unwrap_or_else(|| raw_payload.clone());

        Ok(json!({
            "status": "success",
            "tool_name": tool_name,
            "data": data,
            "error": JsonValue::Null,
            "latency_ms": latency_ms,
        }))
    }
}

#[async_trait]
impl NodeBehavior for ToolExecutorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.tool_executor".to_string(),
            display_name: "Tool Executor".to_string(),
            description: "[SHELL NODE — no runtime behavior] Wrapper node that normalizes MCP tool invocation output. Redundant with core.ai.mcp_invoke / core.ai.mcp_tool (real implementations); Deprecated — prefer those.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Tool invocation plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Normalized tool result plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "tool_name": { "type": "string" }
                },
                "required": ["tool_name"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let tool_name = config["tool_name"]
            .as_str()
            .ok_or_else(|| anyhow!("tool_name is required and must be a string"))?;
        if tool_name.trim().is_empty() {
            return Err(anyhow!("tool_name must not be empty"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Tool Executor requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

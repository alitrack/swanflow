use anyhow::Result;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value as JsonValue;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// A node that bridges to an external MCP (Model Context Protocol) tool.
pub struct McpNode {
    pub tool_name: String,
    // In a real system, this would hold a reference to an McpRegistry
}

#[async_trait]
impl NodeBehavior for McpNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: format!("core.ai.mcp.{}", self.tool_name),
            display_name: format!("MCP Tool: {}", self.tool_name),
            description: "Execute an external AI tool via MCP".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Optional Input Data".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Tool Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "arguments": { "type": "object" }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        // MCP results are typically JSON strings or structured data.
        // We'll need a specialized 'TableProvider' to wrap the MCP call during execution.
        // For Phase 4 MVP, we return an empty plan as a placeholder for the tool link.
        Ok(vec![])
    }
}

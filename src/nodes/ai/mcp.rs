use anyhow::Result;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value as JsonValue;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// A node that bridges to an external MCP (Model Context Protocol) tool.
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
                    "arguments": { "type": "object" }
                },
                "required": ["tool_name"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["tool_name"].as_str().ok_or_else(|| anyhow::anyhow!("tool_name is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        // Placeholder schema for MCP results
        let schema = datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("content", datafusion::arrow::datatypes::DataType::Utf8, true),
            datafusion::arrow::datatypes::Field::new("source", datafusion::arrow::datatypes::DataType::Utf8, true),
        ]);
        let df_schema = datafusion::common::DFSchema::try_from(schema)?;
        
        Ok(vec![LogicalPlan::EmptyRelation(
            datafusion::logical_expr::EmptyRelation {
                produce_one_row: true,
                schema: std::sync::Arc::new(df_schema),
            },
        )])
    }
}

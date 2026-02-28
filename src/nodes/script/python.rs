use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value as JsonValue;
use anyhow::Result;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// A node that executes a Python script for data transformation.
pub struct PythonNode;

#[async_trait]
impl NodeBehavior for PythonNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.script.python".to_string(),
            display_name: "Python Script".to_string(),
            description: "Transform data using a Python script".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Source Table".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Transformed Table".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "script": { "type": "string", "description": "Python code snippet" }
                },
                "required": ["script"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["script"].as_str().ok_or_else(|| anyhow::anyhow!("Python script is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        // In a full implementation, this would involve FFI with pyarrow.
        // For Phase 4 MVP, we act as a pass-through to demonstrate the interface.
        Ok(inputs)
    }
}

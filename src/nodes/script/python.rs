use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value as JsonValue;
use anyhow::{Result, anyhow};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// A node that executes a Python script for data transformation.
pub struct PythonNode;

#[async_trait]
impl NodeBehavior for PythonNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.script.python".to_string(),
            display_name: "Python Script".to_string(),
            description: "Transform data using a Python script with zero-copy".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Source Table".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Transformed Table".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "script": { "type": "string", "description": "Python code: def process(batch): ..." }
                },
                "required": ["script"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["script"].as_str().ok_or_else(|| anyhow!("Python script is required"))?;
        Ok(())
    }

    async fn propose_plan(&self, inputs: Vec<LogicalPlan>, _config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
    }
}

/// A node that registers a Python-based UDF into DataFusion.
pub struct PythonUdfNode;

#[async_trait]
impl NodeBehavior for PythonUdfNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.python_udf".to_string(),
            display_name: "Register Python UDF".to_string(),
            description: "Register a custom Python function as a DataFusion UDF".to_string(),
            input_ports: vec![],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "code": { "type": "string" },
                    "input_types": { "type": "array", "items": { "type": "string" } },
                    "return_type": { "type": "string" }
                },
                "required": ["name", "code", "input_types", "return_type"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["name"].as_str().ok_or_else(|| anyhow!("UDF name is required"))?;
        config["code"].as_str().ok_or_else(|| anyhow!("UDF code is required"))?;
        Ok(())
    }

    async fn propose_plan(&self, _inputs: Vec<LogicalPlan>, _config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        // In prod, this registers the UDF with the SessionContext.
        // For MVP, we treat it as a success placeholder.
        Ok(vec![])
    }
}

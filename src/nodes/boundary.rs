use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use duckdb::Connection;
use serde_json::Value as JsonValue;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Input boundary node for sub-workflows.
pub struct InputNode;

#[async_trait]
impl NodeBehavior for InputNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.input".to_string(),
            display_name: "Input Port".to_string(),
            description: "Entry point for data into a sub-workflow".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Injected Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "port_index": { "type": "integer", "default": 0 }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
    }
}

/// Output boundary node for sub-workflows.
pub struct OutputNode;

#[async_trait]
impl NodeBehavior for OutputNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.output".to_string(),
            display_name: "Output Port".to_string(),
            description: "Exit point for data from a sub-workflow".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Result Data".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "port_index": { "type": "integer", "default": 0 }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
    }
}

/// Load an extension into DuckDB.
pub struct DuckDBExtensionNode;

#[async_trait]
impl NodeBehavior for DuckDBExtensionNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.extension".to_string(),
            display_name: "DuckDB Extension Loader".to_string(),
            description: "Install and load an extension (e.g., httpfs, spatial) into DuckDB"
                .to_string(),
            input_ports: vec![],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "extension": { "type": "string" },
                    "install": { "type": "boolean", "default": true }
                },
                "required": ["extension"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let ext = config["extension"]
            .as_str()
            .ok_or_else(|| anyhow!("Extension name is required"))?;

        const ALLOWED_EXTENSIONS: &[&str] = &[
            "httpfs",
            "spatial",
            "parquet",
            "json",
            "excel",
            "autocomplete",
            "shellfs",
            "fts",
            "tpch",
            "tpcds",
            "icu",
            "inet",
        ];

        if !ALLOWED_EXTENSIONS.contains(&ext) {
            return Err(anyhow!(
                "Unsupported extension: {}. Allowed: {}",
                ext,
                ALLOWED_EXTENSIONS.join(", ")
            ));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let ext = config["extension"].as_str().unwrap();
        let install = config["install"].as_bool().unwrap_or(true);
        let conn = Connection::open_in_memory()?;
        if install {
            let _ = conn.execute_batch(&format!("INSTALL {};", ext));
        }
        let _ = conn.execute_batch(&format!("LOAD {};", ext));
        Ok(vec![])
    }
}

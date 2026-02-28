use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use anyhow::Result;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Read data from a Parquet file.
pub struct ParquetReadNode;

#[async_trait]
impl NodeBehavior for ParquetReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.parquet".to_string(),
            display_name: "Parquet Reader".to_string(),
            description: "Read data from a Parquet file".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["path"].as_str().ok_or_else(|| {
            anyhow::anyhow!("Parquet Path is required")
        })?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

pub struct CsvReadNode;

#[async_trait]
impl NodeBehavior for CsvReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.csv".to_string(),
            display_name: "CSV Reader".to_string(),
            description: "Read data from a CSV file".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["path"].as_str().ok_or_else(|| {
            anyhow::anyhow!("CSV Path is required")
        })?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = ctx.read_csv(path, CsvReadOptions::new()).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

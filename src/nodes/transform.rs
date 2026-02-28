use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use anyhow::Result;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Filter rows based on a SQL expression.
pub struct FilterNode;

#[async_trait]
impl NodeBehavior for FilterNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.filter".to_string(),
            display_name: "Filter".to_string(),
            description: "Filter rows using a SQL expression".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Filtered Data".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "expr": { "type": "string" }
                },
                "required": ["expr"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["expr"].as_str().ok_or_else(|| anyhow::anyhow!("Filter expression is required"))?;
        Ok(())
    }

    async fn propose_plan(&self, inputs: Vec<LogicalPlan>, config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        let expr_str = config["expr"].as_str().unwrap();
        let ctx = SessionContext::new();
        
        // Register the input plan as a temporary table to use SQL for filtering
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("filter_input", df.into_view())?;
        
        // Use SQL to apply the filter
        let sql = format!("SELECT * FROM filter_input WHERE {}", expr_str);
        let filtered_df = ctx.sql(&sql).await?;
        
        Ok(vec![filtered_df.into_optimized_plan()?])
    }
}

/// Project (Select/Rename) columns.
pub struct ProjectNode;

#[async_trait]
impl NodeBehavior for ProjectNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.project".to_string(),
            display_name: "Project".to_string(),
            description: "Select and rename columns".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Projected Data".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "columns": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["columns"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["columns"].as_array().ok_or_else(|| anyhow::anyhow!("Column list is required"))?;
        Ok(())
    }

    async fn propose_plan(&self, inputs: Vec<LogicalPlan>, config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        let cols: Vec<&str> = config["columns"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let df = df.select_columns(&cols)?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

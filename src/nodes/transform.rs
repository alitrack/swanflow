use async_trait::async_trait;
use datafusion::logical_expr::{LogicalPlan, Extension, Aggregate};
use datafusion::prelude::*;
use datafusion::common::{DFSchemaRef, DFSchema};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{ArrayRef, StringBuilder};
use serde_json::Value as JsonValue;
use anyhow::{Result, anyhow};
use std::sync::Arc;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, InferredMetadata};

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
        config["expr"].as_str().ok_or_else(|| anyhow!("Filter expression is required"))?;
        Ok(())
    }

    async fn propose_plan(&self, inputs: Vec<LogicalPlan>, config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        let expr_str = config["expr"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("filter_input", df.into_view())?;
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
        config["columns"].as_array().ok_or_else(|| anyhow!("Column list is required"))?;
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

/// Perform GroupBy and Aggregation.
pub struct AggregateNode;

#[async_trait]
impl NodeBehavior for AggregateNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.aggregate".to_string(),
            display_name: "Aggregate (GroupBy)".to_string(),
            description: "Summarize data by grouping columns and applying aggregation functions".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Summarized Data".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "groupby": { "type": "array", "items": { "type": "string" } },
                    "aggs": { 
                        "type": "object",
                        "description": "Mapping of column to aggregation function (sum, avg, count, min, max)"
                    }
                },
                "required": ["groupby", "aggs"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["groupby"].as_array().ok_or_else(|| anyhow!("GroupBy columns are required"))?;
        config["aggs"].as_object().ok_or_else(|| anyhow!("Aggregations are required"))?;
        Ok(())
    }

    async fn propose_plan(&self, inputs: Vec<LogicalPlan>, config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        let groupby_cols: Vec<&str> = config["groupby"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
        let aggs = config["aggs"].as_object().unwrap();
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let mut select_parts = groupby_cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>();
        for (col, func) in aggs {
            select_parts.push(format!("{}(\"{}\") as {}_{}", func.as_str().unwrap(), col, func.as_str().unwrap(), col));
        }
        ctx.register_table("agg_input", df.into_view())?;
        let sql = format!(
            "SELECT {} FROM agg_input GROUP BY {}", 
            select_parts.join(", "), 
            groupby_cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ")
        );
        let agg_df = ctx.sql(&sql).await?;
        Ok(vec![agg_df.into_optimized_plan()?])
    }
}

/// Perform Window Functions.
pub struct WindowNode;

#[async_trait]
impl NodeBehavior for WindowNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.window".to_string(),
            display_name: "Window Functions".to_string(),
            description: "Apply calculations across a set of rows related to the current row".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Enriched Data".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "partition_by": { "type": "array", "items": { "type": "string" } },
                    "order_by": { "type": "array", "items": { "type": "string" } },
                    "window_exprs": { 
                        "type": "object",
                        "description": "Mapping of output column name to window expression (e.g., 'rank()', 'sum(salary)')"
                    }
                },
                "required": ["window_exprs"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["window_exprs"].as_object().ok_or_else(|| anyhow!("Window expressions are required"))?;
        Ok(())
    }

    async fn propose_plan(&self, inputs: Vec<LogicalPlan>, config: &JsonValue) -> Result<Vec<LogicalPlan>> {
        let partition_by: Vec<String> = config["partition_by"].as_array()
            .map(|v| v.iter().map(|s| format!("\"{}\"", s.as_str().unwrap())).collect())
            .unwrap_or_default();
        let order_by: Vec<String> = config["order_by"].as_array()
            .map(|v| v.iter().map(|s| format!("\"{}\"", s.as_str().unwrap())).collect())
            .unwrap_or_default();
        let window_exprs = config["window_exprs"].as_object().unwrap();
        
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        
        let mut window_selects = Vec::new();
        for (name, expr) in window_exprs {
            let mut clause = format!("{} OVER (", expr.as_str().unwrap());
            if !partition_by.is_empty() {
                clause.push_str(&format!("PARTITION BY {} ", partition_by.join(", ")));
            }
            if !order_by.is_empty() {
                clause.push_str(&format!("ORDER BY {} ", order_by.join(", ")));
            }
            clause.push(')');
            window_selects.push(format!("{} as \"{}\"", clause, name));
        }
        
        ctx.register_table("window_input", df.into_view())?;
        let sql = format!("SELECT *, {} FROM window_input", window_selects.join(", "));
        
        let window_df = ctx.sql(&sql).await?;
        Ok(vec![window_df.into_optimized_plan()?])
    }
}

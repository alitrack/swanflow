use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;

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
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Filtered Data".into(),
            }],
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
        config["expr"]
            .as_str()
            .ok_or_else(|| anyhow!("Filter expression is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
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
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Projected Data".into(),
            }],
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
        config["columns"]
            .as_array()
            .ok_or_else(|| anyhow!("Column list is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let cols: Vec<&str> = config["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
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
            description: "Summarize data by grouping columns and applying aggregation functions. \
                          Mapping: { \"output_col_name\": \"AGG_FUNC(input_col)\" }, e.g., { \"total\": \"SUM(val)\" }".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Summarized Data".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "groupby": { "type": "array", "items": { "type": "string" } },
                    "aggs": { 
                        "type": "object",
                        "description": "Mapping of output name to full SQL aggregate expression, e.g., { \"total_price\": \"SUM(price)\" }"
                    }
                },
                "required": ["groupby", "aggs"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["groupby"]
            .as_array()
            .ok_or_else(|| anyhow!("GroupBy columns are required"))?;
        config["aggs"]
            .as_object()
            .ok_or_else(|| anyhow!("Aggregations are required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let groupby_cols: Vec<String> = config["groupby"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| format!("\"{}\"", v.as_str().unwrap()))
            .collect();
        let aggs = config["aggs"].as_object().unwrap();
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());

        let mut select_parts = groupby_cols.clone();
        for (out_name, agg_expr) in aggs {
            let expr = agg_expr.as_str().unwrap();
            if expr.contains('(') {
                select_parts.push(format!("{} as \"{}\"", expr, out_name));
            } else {
                select_parts.push(format!("{}(\"{}\") as \"{}\"", expr, out_name, out_name));
            }
        }

        ctx.register_table("agg_input", df.into_view())?;
        let sql = if groupby_cols.is_empty() {
            format!("SELECT {} FROM agg_input", select_parts.join(", "))
        } else {
            format!(
                "SELECT {} FROM agg_input GROUP BY {}",
                select_parts.join(", "),
                groupby_cols.join(", ")
            )
        };
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
            description: "Apply calculations across a set of rows related to the current row"
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Enriched Data".into(),
            }],
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
        config["window_exprs"]
            .as_object()
            .ok_or_else(|| anyhow!("Window expressions are required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let partition_by: Vec<String> = config["partition_by"]
            .as_array()
            .map(|v| {
                v.iter()
                    .map(|s| format!("\"{}\"", s.as_str().unwrap()))
                    .collect()
            })
            .unwrap_or_default();
        let order_by: Vec<String> = config["order_by"]
            .as_array()
            .map(|v| {
                v.iter()
                    .map(|s| format!("\"{}\"", s.as_str().unwrap()))
                    .collect()
            })
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

/// Branching based on a SQL boolean expression.
pub struct ConditionNode;

#[async_trait]
impl NodeBehavior for ConditionNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.condition".to_string(),
            display_name: "Condition (IF)".to_string(),
            description:
                "Route data based on a boolean condition. Port 0 for TRUE, Port 1 for FALSE."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to check".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "true".into(),
                    description: "Rows matching condition".into(),
                },
                PortSpec {
                    id: 1,
                    name: "false".into(),
                    description: "Rows not matching condition".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "condition": { "type": "string", "description": "SQL boolean expression (e.g., 'salary > 5000')" }
                },
                "required": ["condition"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["condition"]
            .as_str()
            .ok_or_else(|| anyhow!("Condition is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let condition = config["condition"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("cond_input", df.into_view())?;
        let sql_true = format!("SELECT * FROM cond_input WHERE {}", condition);
        let true_df = ctx.sql(&sql_true).await?;
        let sql_false = format!("SELECT * FROM cond_input WHERE NOT ({})", condition);
        let false_df = ctx.sql(&sql_false).await?;
        Ok(vec![
            true_df.into_optimized_plan()?,
            false_df.into_optimized_plan()?,
        ])
    }
}

/// Loop over a list of items (ForEach).
pub struct ForEachNode;

#[async_trait]
impl NodeBehavior for ForEachNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.foreach".to_string(),
            display_name: "ForEach Loop".to_string(),
            description: "Iterate over a list of values provided in configuration".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "item".into(),
                description: "Current iteration item (as a table)".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "items": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["items"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["items"]
            .as_array()
            .ok_or_else(|| anyhow!("Items list is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let items = config["items"].as_array().unwrap();
        let ctx = SessionContext::new();
        let mut select_parts = Vec::new();
        for item in items {
            select_parts.push(format!("SELECT '{}' as item", item.as_str().unwrap()));
        }
        let sql = select_parts.join(" UNION ALL ");
        let df = ctx.sql(&sql).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

/// Compute statistical profile of input columns.
pub struct ProfilerNode;

#[async_trait]
impl NodeBehavior for ProfilerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.profiler".to_string(),
            display_name: "Data Profiler".to_string(),
            description:
                "Generate statistical profiles (min, max, nulls, unique) for input columns"
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to profile".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "profile".into(),
                description: "Statistical Profile".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "columns": { "type": "array", "items": { "type": "string" } }
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
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let schema = df.schema();

        let cols: Vec<String> = if let Some(c) = config["columns"].as_array() {
            c.iter().map(|v| v.as_str().unwrap().to_string()).collect()
        } else {
            schema.fields().iter().map(|f| f.name().clone()).collect()
        };
        let mut select_parts = Vec::new();
        for col in cols {
            select_parts.push(format!("COUNT(\"{0}\") as \"{0}_count\"", col));
            select_parts.push(format!("COUNT(DISTINCT \"{0}\") as \"{0}_unique\"", col));
            select_parts.push(format!("MIN(\"{0}\") as \"{0}_min\"", col));
            select_parts.push(format!("MAX(\"{0}\") as \"{0}_max\"", col));
            select_parts.push(format!(
                "SUM(CASE WHEN \"{0}\" IS NULL THEN 1 ELSE 0 END) as \"{0}_nulls\"",
                col
            ));
        }
        ctx.register_table("profile_input", df.into_view())?;
        let sql = format!("SELECT {} FROM profile_input", select_parts.join(", "));
        let profile_df = ctx.sql(&sql).await?;
        Ok(vec![profile_df.into_optimized_plan()?])
    }
}

/// Manually define variables.
pub struct VariableCreatorNode;

#[async_trait]
impl NodeBehavior for VariableCreatorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.variable_creator".to_string(),
            display_name: "Variable Creator".to_string(),
            description: "Define one or more variables manually in the configuration".to_string(),
            input_ports: vec![],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "variables": { "type": "object" }
                },
                "required": ["variables"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["variables"]
            .as_object()
            .ok_or_else(|| anyhow!("Variables object is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(vec![])
    }
}

/// Promote table data to variables.
pub struct TableToVariableNode;

#[async_trait]
impl NodeBehavior for TableToVariableNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.table_to_variable".to_string(),
            display_name: "Table to Variable".to_string(),
            description: "Convert the first row of a table into flow variables".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Source Table".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "mapping": {
                        "type": "object",
                        "description": "Mapping of column name to variable name"
                    }
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
        Ok(vec![])
    }
}

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::UserDefinedLogicalNodeCore;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use std::fmt;
use std::sync::Arc;

use crate::core::engine::duckdb_backend::quote_ident;
use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec, TypedNodeBehavior};

/// Filter rows based on a SQL expression.
pub struct FilterNode;

#[async_trait]
impl NodeBehavior for FilterNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.filter".to_string(),
            display_name: "Filter".to_string(),
            description: "Filter rows based on a SQL expression.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to filter".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Filtered data".into(),
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
        cfg::required_string(config, "expr")?;
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("FilterNode requires at least one input");
        }
        let expr_str = cfg::required_string(config, "expr")?;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("filter_input", df.into_view())?;
        let df = ctx
            .sql(&format!("SELECT * FROM filter_input WHERE {}", expr_str))
            .await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

impl TypedNodeBehavior for FilterNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

/// Project/Rename columns using SQL expressions.
pub struct ProjectNode;

#[async_trait]
impl NodeBehavior for ProjectNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.project".to_string(),
            display_name: "Project".to_string(),
            description: "Select and rename columns using SQL expressions.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Input data".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Projected data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "exprs": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["exprs"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_array(config, "exprs")?;
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("ProjectNode requires at least one input");
        }
        let select_cols = cfg::required_string_array(config, "exprs")?;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("project_input", df.into_view())?;
        // P2-1a: quote each column identifier to close name-based SQL injection.
        let cols: Vec<String> = select_cols.iter().map(|c| quote_ident(c)).collect();
        let sql = format!("SELECT {} FROM project_input", cols.join(", "));
        let projected = ctx.sql(&sql).await?;
        Ok(vec![projected.into_optimized_plan()?])
    }
}

pub struct AggregateNode;

#[async_trait]
impl NodeBehavior for AggregateNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.aggregate".to_string(),
            display_name: "Aggregate".to_string(),
            description: "Compute aggregations (sum, count, etc) over groups.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to aggregate".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Aggregated data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "group_by": { "type": "array", "items": { "type": "string" } },
                    "aggs": { "type": "array", "items": { "type": "string" } }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let group_by = cfg::optional_string_array(config, "group_by")?;
        let aggs = cfg::optional_string_array(config, "aggs")?;
        if group_by.is_empty() && aggs.is_empty() {
            anyhow::bail!(
                "AggregateNode requires at least one of 'group_by' or 'aggs' to be non-empty"
            );
        }
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("AggregateNode requires at least one input");
        }
        let group_by = cfg::optional_string_array(config, "group_by")?;
        let aggs = cfg::optional_string_array(config, "aggs")?;

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("agg_input", df.into_view())?;

        let mut sql = "SELECT ".to_string();
        if !group_by.is_empty() {
            sql += &group_by.join(", ");
            if !aggs.is_empty() {
                sql += ", ";
            }
        }
        sql += &aggs.join(", ");
        sql += " FROM agg_input";
        if !group_by.is_empty() {
            sql += " GROUP BY ";
            sql += &group_by.join(", ");
        }

        let agg_df = ctx.sql(&sql).await?;
        Ok(vec![agg_df.into_optimized_plan()?])
    }
}

pub struct WindowNode;

#[async_trait]
impl NodeBehavior for WindowNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.window".to_string(),
            display_name: "Window Functions".to_string(),
            description: "Apply window functions like RANK, ROW_NUMBER, etc.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Input data".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Data with window results".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "window_exprs": { "type": "array", "items": { "type": "string" } }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("WindowNode requires at least one input");
        }
        let exprs = cfg::required_string_array(config, "window_exprs")?;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("window_input", df.into_view())?;
        let sql = format!("SELECT *, {} FROM window_input", exprs.join(", "));
        let window_df = ctx.sql(&sql).await?;
        Ok(vec![window_df.into_optimized_plan()?])
    }
}

pub struct ConditionNode;

#[async_trait]
impl NodeBehavior for ConditionNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.condition".to_string(),
            display_name: "Condition (IF)".to_string(),
            description: "Branch workflow based on a boolean condition.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to branch".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "true".into(),
                    description: "Executed if condition is true".into(),
                },
                PortSpec {
                    id: 1,
                    name: "false".into(),
                    description: "Executed if condition is false".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "expr": { "type": "string" }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "expr")?;
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("ConditionNode requires at least one input");
        }
        let expr_str = cfg::required_string(config, "expr")?;

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("cond_input", df.into_view())?;

        // True branch: rows matching the condition expression
        let true_sql = format!("SELECT * FROM cond_input WHERE {}", expr_str);
        let true_df = ctx.sql(&true_sql).await?;

        // False branch: rows that do NOT match the condition
        let false_sql = format!("SELECT * FROM cond_input WHERE NOT ({})", expr_str);
        let false_df = ctx.sql(&false_sql).await?;

        Ok(vec![
            true_df.into_optimized_plan()?,
            false_df.into_optimized_plan()?,
        ])
    }
}

pub struct ForEachNode;

#[async_trait]
impl NodeBehavior for ForEachNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.foreach".to_string(),
            display_name: "For Each (Map)".to_string(),
            description: "Process each item in a collection separately.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "items".into(),
                description: "Collection of items".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Processed items".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "partition_column": {
                        "type": "string",
                        "description": "Column to partition/group items by for iteration. If set, data is ordered by this column before processing."
                    }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("ForEachNode requires at least one input");
        }
        let partition_col = cfg::optional_string(config, "partition_column");

        let plan = if let Some(ref col) = partition_col {
            let ctx = SessionContext::new();
            let df = DataFrame::new(ctx.state(), inputs[0].clone());
            ctx.register_table("foreach_input", df.into_view())?;
            // Order by the partition column so rows are grouped together
            let sql = format!("SELECT * FROM foreach_input ORDER BY \"{}\"", col);
            ctx.sql(&sql).await?.into_optimized_plan()?
        } else {
            // No partition column: pass through as-is
            inputs[0].clone()
        };

        Ok(vec![plan])
    }
}

pub struct ProfilerNode;

#[async_trait]
impl NodeBehavior for ProfilerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.profiler".to_string(),
            display_name: "Data Profiler".to_string(),
            description: "Compute column-level statistics (min, max, unique, nulls).".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to profile".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "profile".into(),
                description: "Statistical summary".into(),
            }],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("ProfilerNode requires at least one input");
        }
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let schema = df.schema();

        let mut select_parts = Vec::new();
        for field in schema.fields() {
            let col = field.name();
            select_parts.push(format!("COUNT(\"{0}\") as \"{0}_count\"", col));
            select_parts.push(format!("COUNT(DISTINCT \"{0}\") as \"{0}_unique\"", col));
            select_parts.push(format!("MIN(\"{0}\") as \"{0}_min\"", col));
            select_parts.push(format!("MAX(\"{0}\") as \"{0}_max\"", col));
            select_parts.push(format!(
                "SUM(CASE WHEN \"{0}\" IS NULL THEN 1 ELSE 0 END) as \"{0}_nulls\"",
                col
            ));
            select_parts.push(format!(
                "CAST(SUM(CASE WHEN \"{0}\" IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as \"{0}_null_ratio\"",
                col
            ));
            // AVG only for numeric columns: DataFusion has no implicit
            // Utf8->Float64 coercion for avg (caught by wf157 E2E:
            // profiler on a schema with string columns failed planning).
            let is_numeric = matches!(
                field.data_type(),
                datafusion::arrow::datatypes::DataType::Int8
                    | datafusion::arrow::datatypes::DataType::Int16
                    | datafusion::arrow::datatypes::DataType::Int32
                    | datafusion::arrow::datatypes::DataType::Int64
                    | datafusion::arrow::datatypes::DataType::UInt8
                    | datafusion::arrow::datatypes::DataType::UInt16
                    | datafusion::arrow::datatypes::DataType::UInt32
                    | datafusion::arrow::datatypes::DataType::UInt64
                    | datafusion::arrow::datatypes::DataType::Float16
                    | datafusion::arrow::datatypes::DataType::Float32
                    | datafusion::arrow::datatypes::DataType::Float64
                    | datafusion::arrow::datatypes::DataType::Decimal128(_, _)
                    | datafusion::arrow::datatypes::DataType::Decimal256(_, _)
            );
            if is_numeric {
                select_parts.push(format!("AVG(\"{0}\") as \"{0}_avg\"", col));
            }
        }

        ctx.register_table("profile_input", df.into_view())?;
        let sql = format!("SELECT {} FROM profile_input", select_parts.join(", "));
        let profile_df = ctx.sql(&sql).await?;
        Ok(vec![profile_df.into_optimized_plan()?])
    }
}

pub struct VariableCreatorNode;

#[async_trait]
impl NodeBehavior for VariableCreatorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.variable_creator".to_string(),
            display_name: "Variable Creator".to_string(),
            description: "Create workflow variables from configuration.".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "variables".into(),
                description: "Single-row DataFrame with variable values".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": { "type": "string" }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if config.as_object().map_or(0, |m| m.len()) == 0 {
            anyhow::bail!("VariableCreatorNode requires at least one key/value pair in config");
        }
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let obj = config
            .as_object()
            .ok_or_else(|| anyhow!("VariableCreatorNode config must be a JSON object"))?;

        if obj.is_empty() {
            anyhow::bail!("VariableCreatorNode requires at least one variable definition");
        }

        // Build a SQL SELECT with literal values for each key/value pair.
        // NOTE: the compiler injects reserved metadata keys (e.g. impl_version)
        // into the config object before it reaches the node — skip anything
        // that is not a string-valued user variable instead of erroring.
        // Caught by wf64 E2E (Variable 'impl_version' must be a string).
        const RESERVED: &[&str] = &["impl_version", "config_version"];
        let mut select_parts = Vec::new();
        let mut user_vars = 0usize;
        for (key, value) in obj {
            if RESERVED.contains(&key.as_str()) {
                continue;
            }
            let val_str = value
                .as_str()
                .ok_or_else(|| anyhow!("Variable '{}' must be a string", key))?;
            user_vars += 1;
            // Escape single quotes in the value by doubling them
            let escaped = val_str.replace('\'', "''");
            select_parts.push(format!("'{}' as \"{}\"", escaped, key));
        }
        if user_vars == 0 {
            anyhow::bail!("VariableCreatorNode requires at least one string-valued variable");
        }

        let ctx = SessionContext::new();
        let sql = format!("SELECT {}", select_parts.join(", "));
        let df = ctx.sql(&sql).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

pub struct TableToVariableNode;

#[async_trait]
impl NodeBehavior for TableToVariableNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.table_to_variable".to_string(),
            display_name: "Table to Variable".to_string(),
            description: "Convert a single-row table into workflow variables.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Single-row table".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "variables".into(),
                description: "Single-row DataFrame with column names as variable keys".into(),
            }],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("TableToVariableNode requires at least one input");
        }
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("ttv_input", df.into_view())?;
        // Select the first row to convert columns into variable values
        let output = ctx.sql("SELECT * FROM ttv_input LIMIT 1").await?;
        Ok(vec![output.into_optimized_plan()?])
    }
}

pub struct GraphMergerNode;

#[async_trait]
impl NodeBehavior for GraphMergerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.graph_merger".to_string(),
            display_name: "Graph Merger".to_string(),
            description: "Merge multiple graph node/edge streams with conflict resolution."
                .to_string(),
            input_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes_a".into(),
                    description: "Nodes Set A".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges_a".into(),
                    description: "Edges Set A".into(),
                },
                PortSpec {
                    id: 2,
                    name: "nodes_b".into(),
                    description: "Nodes Set B".into(),
                },
                PortSpec {
                    id: 3,
                    name: "edges_b".into(),
                    description: "Edges Set B".into(),
                },
            ],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes".into(),
                    description: "Merged Nodes".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges".into(),
                    description: "Merged Edges".into(),
                },
            ],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.len() < 4 {
            anyhow::bail!(
                "GraphMergerNode requires at least 4 inputs (nodes_a, edges_a, nodes_b, edges_b)"
            );
        }
        let ctx = SessionContext::new();

        let nodes_a = inputs[0].clone();
        let edges_a = inputs[1].clone();
        let nodes_b = inputs[2].clone();
        let edges_b = inputs[3].clone();

        ctx.register_table("nodes_a", DataFrame::new(ctx.state(), nodes_a).into_view())?;
        ctx.register_table("nodes_b", DataFrame::new(ctx.state(), nodes_b).into_view())?;
        ctx.register_table("edges_a", DataFrame::new(ctx.state(), edges_a).into_view())?;
        ctx.register_table("edges_b", DataFrame::new(ctx.state(), edges_b).into_view())?;

        // NOTE: DataFusion has no ANY_VALUE() (DuckDB-only) — use min() which
        // both engines support; for duplicate-id resolution semantics are
        // "pick one deterministic value" either way (bug: ANY_VALUE made this
        // node unplannable in DataFusion from day one).
        let nodes_merged = ctx.sql("SELECT id, min(label) as label, min(properties) as properties FROM (SELECT * FROM nodes_a UNION ALL SELECT * FROM nodes_b) GROUP BY id").await?;
        let edges_merged = ctx.sql("SELECT source_id, target_id, type, SUM(weight) as weight FROM (SELECT * FROM edges_a UNION ALL SELECT * FROM edges_b) GROUP BY source_id, target_id, type").await?;

        Ok(vec![
            nodes_merged.into_optimized_plan()?,
            edges_merged.into_optimized_plan()?,
        ])
    }
}

pub struct TableSchemaValidatorNode;

#[async_trait]
impl NodeBehavior for TableSchemaValidatorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.table_schema_validator".to_string(),
            display_name: "Table Schema Validator".to_string(),
            description: "Map arbitrary input tables to the standardized Golden Graph Schema (Node/Edge) using LLM mapping.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Arbitrary Data".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes".into(),
                    description: "Standardized Nodes".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges".into(),
                    description: "Standardized Edges".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string", "enum": ["node", "edge", "auto"], "default": "auto" },
                    "mapping": {
                        "type": "object",
                        "description": "Optional explicit mapping. If missing, LLM will suggest one based on schema."
                    }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("TableSchemaValidatorNode requires at least one input");
        }
        use crate::nodes::GraphSchemas;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let input_schema: datafusion::arrow::datatypes::SchemaRef =
            Arc::new(df.schema().as_arrow().clone());

        let to_sql_type = |dt: &datafusion::arrow::datatypes::DataType| match dt {
            datafusion::arrow::datatypes::DataType::Int64 => "BIGINT",
            datafusion::arrow::datatypes::DataType::Utf8 => "TEXT",
            datafusion::arrow::datatypes::DataType::Float64 => "DOUBLE",
            _ => "TEXT",
        };

        let mapping = config["mapping"].as_object();

        let node_plan = if let Some(m) = mapping {
            let mut projections = Vec::new();
            let node_fields = GraphSchemas::node_schema().fields().clone();
            for field in &node_fields {
                let field_name = field.name().as_str();
                let sql_type = to_sql_type(field.data_type());
                if let Some(src_col) = m.get(field_name) {
                    let src_col = src_col.as_str().ok_or_else(|| {
                        anyhow!(
                            "mapping field '{}' must map to a string column name",
                            field_name
                        )
                    })?;
                    projections.push(format!(
                        "CAST(\"{}\" AS {}) as \"{}\"",
                        src_col, sql_type, field_name
                    ));
                } else {
                    projections.push(format!("CAST(NULL AS {}) as \"{}\"", sql_type, field_name));
                }
            }
            ctx.register_table("val_input", df.clone().into_view())?;
            let sql = format!("SELECT {} FROM val_input", projections.join(", "));
            ctx.sql(&sql).await?.into_optimized_plan()?
        } else {
            let mut projections = Vec::new();
            for field in GraphSchemas::node_schema().fields() {
                let sql_type = to_sql_type(field.data_type());
                if input_schema.field_with_name(field.name()).is_ok() {
                    projections.push(format!(
                        "CAST(\"{}\" AS {}) as \"{}\"",
                        field.name(),
                        sql_type,
                        field.name()
                    ));
                } else {
                    projections.push(format!(
                        "CAST(NULL AS {}) as \"{}\"",
                        sql_type,
                        field.name()
                    ));
                }
            }
            ctx.register_table("val_input_auto", df.clone().into_view())?;
            let sql = format!("SELECT {} FROM val_input_auto", projections.join(", "));
            ctx.sql(&sql).await?.into_optimized_plan()?
        };

        let edge_plan = {
            let mut projections = Vec::new();
            for field in GraphSchemas::edge_schema().fields() {
                let sql_type = to_sql_type(field.data_type());
                if input_schema.field_with_name(field.name()).is_ok() {
                    projections.push(format!(
                        "CAST(\"{}\" AS {}) as \"{}\"",
                        field.name(),
                        sql_type,
                        field.name()
                    ));
                } else {
                    projections.push(format!(
                        "CAST(NULL AS {}) as \"{}\"",
                        sql_type,
                        field.name()
                    ));
                }
            }
            ctx.register_table("val_input_edge", df.into_view())?;
            let sql = format!("SELECT {} FROM val_input_edge", projections.join(", "));
            ctx.sql(&sql).await?.into_optimized_plan()?
        };

        Ok(vec![node_plan, edge_plan])
    }
}

pub struct SemanticCleanerNode;

#[async_trait]
impl NodeBehavior for SemanticCleanerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.semantic_cleaner".to_string(),
            display_name: "Semantic Cleaner".to_string(),
            description:
                "Clean semantic inconsistencies using AI-generated rules based on a data sample."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to clean".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Cleaned Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "column": { "type": "string" },
                    "rules": {
                        "type": "object",
                        "description": "Mapping of incorrect values to correct ones (generated by AI)."
                    },
                    "sample_size": { "type": "integer", "default": 100 }
                },
                "required": ["column"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "column")?;
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("SemanticCleanerNode requires at least one input");
        }
        let column = cfg::required_string(config, "column")?;
        let rules = config["rules"].as_object();

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());

        if let Some(r) = rules {
            let mut case_parts = Vec::new();
            for (incorrect, correct) in r {
                case_parts.push(format!(
                    "WHEN \"{}\" = '{}' THEN '{}'",
                    column,
                    incorrect,
                    correct.as_str().unwrap_or("")
                ));
            }

            if case_parts.is_empty() {
                return Ok(vec![inputs[0].clone()]);
            }

            let sql = format!(
                "SELECT *, CASE {} ELSE \"{}\" END as \"{}_cleaned\" FROM cleaner_input",
                case_parts.join(" "),
                column,
                column
            );
            ctx.register_table("cleaner_input", df.into_view())?;
            let cleaned_df = ctx.sql(&sql).await?;
            Ok(vec![cleaned_df.into_optimized_plan()?])
        } else {
            Ok(vec![inputs[0].clone()])
        }
    }
}

pub struct AiRouterNode;

#[async_trait]
impl NodeBehavior for AiRouterNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.ai_router".to_string(),
            display_name: "AI Router".to_string(),
            description: "Inspect data profile and dynamically route to downstream branches."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to route".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "branch_a".into(),
                    description: "Primary branch".into(),
                },
                PortSpec {
                    id: 1,
                    name: "branch_b".into(),
                    description: "Secondary branch".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "condition_script": { "type": "string", "description": "Optional AI prompt or logic to determine branch." },
                    "selected_branch": { "type": "integer", "default": 0 }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("AiRouterNode requires at least one input");
        }
        let selected = cfg::optional_number(config, "selected_branch").unwrap_or(0.0) as usize;

        use datafusion::logical_expr::EmptyRelation;

        let mut plans = Vec::new();
        for i in 0..2 {
            if i == selected {
                plans.push(inputs[0].clone());
            } else {
                plans.push(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: inputs[0].schema().clone(),
                }));
            }
        }

        Ok(plans)
    }
}

/// Pivot table: group by row columns, optionally split by a column value
/// (one output column per distinct value), aggregate the value column.
///
/// SQL translation (DataFusion):
/// `SELECT r1, r2, SUM(CASE WHEN c = 'v' THEN v END) AS "c_v", ... FROM in GROUP BY r1, r2`
/// Pivot execution proxy (same pattern as `SqlProxyLogical` in `sql.rs`).
///
/// `PivotNode::propose_plan` probes the upstream data to determine the distinct
/// `column` values (the pivot's output schema is data-dependent: one column per
/// distinct value). It returns this extension node carrying the fully-rendered
/// pivot SQL plus the probed output schema. At physical planning time, the
/// engine's `SwanFlowPlanner` re-executes the SQL in the shared engine context
/// where the physical inputs are re-registered as `input_0`, so upstream
/// `MaterializerLogical`/`StateProbeLogical`/source nodes are handled by the
/// engine's own machinery (late binding) rather than being embedded into a
/// separately-built plan — which the physical planner cannot convert.
#[derive(Debug, Clone)]
pub struct PivotProxyLogical {
    pub node_id: uuid::Uuid,
    pub query: String,
    pub inputs: Vec<LogicalPlan>,
    pub schema: DFSchemaRef,
    /// Full materialized snapshot of the upstream, probed at plan time.
    /// Retained for debugging / parity with the probed schema.
    pub snapshot: Vec<RecordBatch>,
    /// The pivot RESULT, computed at plan time. The runtime handler returns
    /// a MemoryExec over these batches — fully self-contained, no runtime SQL,
    /// no dependency on the shared (mutable) `input_0` table state at run time.
    pub result: Vec<RecordBatch>,
}

impl std::cmp::PartialEq for PivotProxyLogical {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.query == other.query
    }
}
impl std::cmp::Eq for PivotProxyLogical {}
impl std::hash::Hash for PivotProxyLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        self.query.hash(state);
    }
}
impl PartialOrd for PivotProxyLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PivotProxyLogical {
    fn name(&self) -> &str {
        "PivotProxy"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PivotProxy(node_id={})", self.node_id)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            node_id: self.node_id,
            query: self.query.clone(),
            inputs,
            schema: self.schema.clone(),
            snapshot: self.snapshot.clone(),
            result: self.result.clone(),
        })
    }
}

/// Execute a logical plan using the engine's extension-aware physical planner,
/// so upstream custom nodes (`MaterializerLogical`, `StateProbeLogical`,
/// `DuckDBScanLogical`, `ExcelScanLogical`, ...) are converted correctly.
///
/// A bare `SessionContext` cannot execute those custom nodes (it has no
/// extension planner), which is exactly why the old `PivotNode` failed at run
/// time: it registered the upstream plan in a throwaway ctx and ran SQL over
/// it. Mirrors `WorkflowCompiler::execute_plan`.
async fn pivot_execute_plan(plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
    let ctx = Arc::new(SessionContext::new());
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let planner = crate::core::engine::SwanFlowPlanner {
        event_tx: tx,
        ctx: ctx.clone(),
        cache_dir: "target/swanflow_cache".to_string(),
    };
    let physical_planner = Arc::new(datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(
        vec![Arc::new(planner)],
    ));
    let state = datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state())
        .with_query_planner(Arc::new(crate::core::engine::SwanFlowQueryPlanner {
            physical_planner,
        }))
        .build();
    let physical_plan = state.create_physical_plan(plan).await?;
    let batches = datafusion::physical_plan::collect(physical_plan, state.task_ctx()).await?;
    Ok(batches)
}

/// Extract the distinct string values of `column` from `batches` (ordered).
fn distinct_string_values(batches: &[RecordBatch], column: &str) -> Result<Vec<String>> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for batch in batches {
        let col_arr = batch
            .column_by_name(column)
            .ok_or_else(|| anyhow!("pivot column '{}' not in input", column))?;
        match col_arr.data_type() {
            datafusion::arrow::datatypes::DataType::Utf8 => {
                let arr = col_arr
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap();
                for v in arr {
                    if let Some(s) = v {
                        if seen.insert(s.to_string()) {
                            out.push(s.to_string());
                        }
                    }
                }
            }
            datafusion::arrow::datatypes::DataType::LargeUtf8 => {
                return Err(anyhow!(
                    "PivotNode column '{}' must be a Utf8 column, got LargeUtf8",
                    column
                ));
            }
            other => {
                return Err(anyhow!(
                    "PivotNode column '{}' must be a string column, got {:?}",
                    column,
                    other
                ));
            }
        }
    }
    out.sort();
    Ok(out)
}

pub struct PivotNode;

#[async_trait]
impl NodeBehavior for PivotNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.pivot".to_string(),
            display_name: "Pivot Table".to_string(),
            description:
                "Pivot data: group by rows, split by column values, aggregate value column."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to pivot".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Pivoted table".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "row": { "type": "string", "description": "Row grouping column." },
                    "column": { "type": "string", "description": "Column to spread (one output col per distinct value). Optional." },
                    "value": { "type": "string", "description": "Value column to aggregate. Defaults to COUNT(*)." },
                    "aggregator": { "type": "string", "enum": ["sum", "avg", "min", "max", "count"], "default": "sum" }
                },
                "required": ["row"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "row")?;
        let agg = cfg::optional_string(config, "aggregator").unwrap_or_else(|| "sum".to_string());
        if !matches!(
            agg.as_str(),
            "sum" | "avg" | "min" | "max" | "count"
        ) {
            anyhow::bail!(
                "PivotNode aggregator must be one of sum/avg/min/max/count, got '{agg}'"
            );
        }
        if let Some(value) = cfg::optional_string(config, "value") {
            if value.trim().is_empty() {
                anyhow::bail!("PivotNode 'value' must be non-empty when set");
            }
        }
        Ok(())
    }

    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            anyhow::bail!("PivotNode requires at least one input");
        }
        let row = cfg::required_string(config, "row")?;
        let column = cfg::optional_string(config, "column");
        let value = cfg::optional_string(config, "value");
        let aggregator = cfg::optional_string(config, "aggregator")
            .unwrap_or_else(|| "sum".to_string());

        let q = |s: &str| format!("\"{}\"", s);

        // The pivot's output schema is data-dependent (one output column per
        // distinct `column` value), so we must materialize the upstream once at
        // plan time to discover the distinct values. Use the engine's
        // extension-aware planner so upstream custom nodes (Materializer /
        // StateProbe / DuckDBScan / ExcelScan / csv) execute correctly — a bare
        // SessionContext cannot convert them (this was the old run-time bug).
        let data = pivot_execute_plan(&inputs[0]).await?;
        if data.iter().all(|b| b.num_rows() == 0) {
            anyhow::bail!("PivotNode input produced no rows; cannot determine pivot columns");
        }

        let distinct_values: Vec<String> = match column.as_deref() {
            Some(col) => distinct_string_values(&data, col)?,
            None => Vec::new(),
        };

        // Render the pivot SQL against the engine input alias `input_0`. At
        // runtime the planner re-registers the upstream as `input_0` and
        // re-executes this (late binding, same pattern as SqlProxyLogical).
        let mut select_parts = vec![q(&row)];
        if let Some(col) = column.as_deref() {
            let inner = match value.as_deref() {
                Some(v) => q(v),
                None => "1".to_string(),
            };
            for v in &distinct_values {
                let label = format!("{}__{}", col, v);
                select_parts.push(format!(
                    "{}(CASE WHEN {} = '{}' THEN {} END) AS {}",
                    aggregator.to_uppercase(),
                    q(col),
                    v.replace('\'', "''"),
                    inner,
                    q(&label)
                ));
            }
        } else if let Some(v) = value.as_deref() {
            select_parts.push(format!(
                "{}({}) AS {}",
                aggregator.to_uppercase(),
                q(v),
                q(&format!("{}_{}", aggregator, v))
            ));
        } else {
            select_parts.push("COUNT(*) AS row_count".to_string());
        }

        let sql = format!(
            "SELECT {} FROM input_0 GROUP BY {} ORDER BY {}",
            select_parts.join(", "),
            q(&row),
            q(&row)
        );

        // Validate the SQL, derive the output schema, and COMPUTE THE RESULT
        // at plan time by running it against the materialized snapshot
        // (registered as `input_0`). Storing the result means the runtime
        // handler can return a self-contained MemoryExec — no runtime SQL, no
        // dependency on the shared mutable `input_0` table state at run time.
        let probe_ctx = SessionContext::new();
        let snapshot_schema = data[0].schema();
        let memtable = datafusion::datasource::MemTable::try_new(
            snapshot_schema,
            vec![data.clone()],
        )?;
        probe_ctx.register_table("input_0", Arc::new(memtable))?;
        let out_df = probe_ctx.sql(&sql).await?;
        let schema: DFSchemaRef = Arc::new(out_df.schema().clone());
        let result = out_df.collect().await?;
        if result.iter().all(|b| b.num_rows() == 0) {
            // No rows (e.g. single row-col with no values) is still a valid
            // (empty) pivot; only an upstream with zero rows is an error, which
            // we already guard above.
        }

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(PivotProxyLogical {
                node_id: uuid::Uuid::nil(),
                query: sql,
                inputs: vec![inputs[0].clone()],
                schema,
                snapshot: data,
                result,
            }),
        })])
    }
}

#[cfg(test)]
mod pivot_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use serde_json::json;

    fn sample_batch() -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("product", DataType::Utf8, false),
            Field::new("sales", DataType::Int64, false),
        ]));
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["east", "east", "west", "west"])),
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )?
    )}

    async fn plan_from_batch(batch: &RecordBatch) -> Result<LogicalPlan> {
        let ctx = SessionContext::new();
        let provider = datafusion::datasource::MemTable::try_new(
            batch.schema(),
            vec![vec![batch.clone()]],
        )?;
        ctx.register_table("t", Arc::new(provider))?;
        Ok(ctx.table("t").await?.into_optimized_plan()?)
    }

    /// Execute a plan through the engine's extension-aware physical planner
    /// (PivotNode now returns a PivotProxyLogical extension node, which a bare
    /// SessionContext cannot convert). Mirrors WorkflowCompiler::execute_plan.
    async fn collect_plan(plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let ctx = Arc::new(SessionContext::new());
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: tx,
            ctx: ctx.clone(),
            cache_dir: "target/swanflow_cache".to_string(),
        };
        let physical_planner = Arc::new(
            datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::new(planner),
            ]),
        );
        let state = datafusion::execution::SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(Arc::new(crate::core::engine::SwanFlowQueryPlanner {
                physical_planner,
            }))
            .build();
        let physical_plan = state.create_physical_plan(plan).await?;
        Ok(datafusion::physical_plan::collect(physical_plan, state.task_ctx()).await?)
    }

    #[tokio::test]
    async fn test_pivot_spreads_column_values() -> Result<()> {
        let node = PivotNode;
        let plan = plan_from_batch(&sample_batch()?).await?;
        let config = json!({
            "row": "region",
            "column": "product",
            "value": "sales",
            "aggregator": "sum"
        });
        let plans = node.propose_plan(vec![plan], &config).await?;
        assert_eq!(plans.len(), 1);

        let batches = collect_plan(&plans[0]).await?;
        let mut rows: Vec<(String, i64, i64)> = Vec::new();
        for b in &batches {
            let region = b
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let pa = b
                .column_by_name("product__a")
                .ok_or_else(|| anyhow!("missing product__a"))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let pb = b
                .column_by_name("product__b")
                .ok_or_else(|| anyhow!("missing product__b"))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    region.value(i).to_string(),
                    pa.value(i),
                    pb.value(i),
                ));
            }
        }
        rows.sort();
        assert_eq!(rows, vec![("east".into(), 10, 20), ("west".into(), 30, 40)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_pivot_plain_groupby_without_column() -> Result<()> {
        let node = PivotNode;
        let plan = plan_from_batch(&sample_batch()?).await?;
        let config = json!({"row": "region", "value": "sales", "aggregator": "count"});
        let plans = node.propose_plan(vec![plan], &config).await?;
        let batches = collect_plan(&plans[0]).await?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "two regions");
        Ok(())
    }

    #[test]
    fn test_pivot_validate_rejects_bad_aggregator() {
        let node = PivotNode;
        let config = json!({"row": "region", "aggregator": "median"});
        assert!(node.validate(&config).is_err());
    }

    #[test]
    fn test_pivot_validate_requires_row() {
        let node = PivotNode;
        let config = json!({"value": "sales"});
        assert!(node.validate(&config).is_err());
    }
}

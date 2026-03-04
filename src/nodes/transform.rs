use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Filter rows based on a SQL expression.
pub struct FilterNode;

#[async_trait]
impl NodeBehavior for FilterNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.filter".to_string(),
            display_name: "Filter".to_string(),
            description: "Filter rows based on a SQL expression.".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data to filter".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Filtered data".into() }],
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

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
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

/// Project/Rename columns using SQL expressions.
pub struct ProjectNode;

#[async_trait]
impl NodeBehavior for ProjectNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.project".to_string(),
            display_name: "Project".to_string(),
            description: "Select and rename columns using SQL expressions.".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Input data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Projected data".into() }],
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

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let select_cols = cfg::required_string_array(config, "exprs")?;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("project_input", df.into_view())?;
        let sql = format!("SELECT {} FROM project_input", select_cols.join(", "));
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
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data to aggregate".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Aggregated data".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "group_by": { "type": "array", "items": { "type": "string" } },
                    "aggs": { "type": "array", "items": { "type": "string" } }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let group_by = cfg::optional_string_array(config, "group_by")?;
        let aggs = cfg::optional_string_array(config, "aggs")?;
        
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("agg_input", df.into_view())?;
        
        let mut sql = "SELECT ".to_string();
        if !group_by.is_empty() {
            sql += &group_by.join(", ");
            if !aggs.is_empty() { sql += ", "; }
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
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Input data".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Data with window results".into() }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "window_exprs": { "type": "array", "items": { "type": "string" } }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
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
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data to branch".into() }],
            output_ports: vec![
                PortSpec { id: 0, name: "true".into(), description: "Executed if condition is true".into() },
                PortSpec { id: 1, name: "false".into(), description: "Executed if condition is false".into() },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "expr": { "type": "string" }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(vec![inputs[0].clone(), inputs[0].clone()])
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
            input_ports: vec![PortSpec { id: 0, name: "items".into(), description: "Collection of items".into() }],
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Processed items".into() }],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
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
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data to profile".into() }],
            output_ports: vec![PortSpec { id: 0, name: "profile".into(), description: "Statistical summary".into() }],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
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
            select_parts.push(format!("AVG(\"{0}\") as \"{0}_avg\"", col));
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
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": { "type": "string" }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(vec![])
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
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Single-row table".into() }],
            output_ports: vec![],
            config_schema: serde_json::json!({}),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> { Ok(()) }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
    }
}

pub struct GraphMergerNode;

#[async_trait]
impl NodeBehavior for GraphMergerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.graph_merger".to_string(),
            display_name: "Graph Merger".to_string(),
            description: "Merge multiple graph node/edge streams with conflict resolution.".to_string(),
            input_ports: vec![
                PortSpec { id: 0, name: "nodes_a".into(), description: "Nodes Set A".into() },
                PortSpec { id: 1, name: "edges_a".into(), description: "Edges Set A".into() },
                PortSpec { id: 2, name: "nodes_b".into(), description: "Nodes Set B".into() },
                PortSpec { id: 3, name: "edges_b".into(), description: "Edges Set B".into() },
            ],
            output_ports: vec![
                PortSpec { id: 0, name: "nodes".into(), description: "Merged Nodes".into() },
                PortSpec { id: 1, name: "edges".into(), description: "Merged Edges".into() },
            ],
            config_schema: serde_json::json!({}),
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
        let ctx = SessionContext::new();
        
        let nodes_a = inputs[0].clone();
        let edges_a = inputs[1].clone();
        let nodes_b = inputs[2].clone();
        let edges_b = inputs[3].clone();

        ctx.register_table("nodes_a", DataFrame::new(ctx.state(), nodes_a).into_view())?;
        ctx.register_table("nodes_b", DataFrame::new(ctx.state(), nodes_b).into_view())?;
        ctx.register_table("edges_a", DataFrame::new(ctx.state(), edges_a).into_view())?;
        ctx.register_table("edges_b", DataFrame::new(ctx.state(), edges_b).into_view())?;

        let nodes_merged = ctx.sql("SELECT id, ANY_VALUE(label) as label, ANY_VALUE(properties) as properties FROM (SELECT * FROM nodes_a UNION ALL SELECT * FROM nodes_b) GROUP BY id").await?;
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

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        use crate::nodes::GraphSchemas;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let input_schema: datafusion::arrow::datatypes::SchemaRef = Arc::new(df.schema().as_arrow().clone());

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
                     let src_col = src_col
                         .as_str()
                         .ok_or_else(|| anyhow!("mapping field '{}' must map to a string column name", field_name))?;
                     projections.push(format!("CAST(\"{}\" AS {}) as \"{}\"", src_col, sql_type, field_name));
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
                    projections.push(format!("CAST(\"{}\" AS {}) as \"{}\"", field.name(), sql_type, field.name()));
                } else {
                    projections.push(format!("CAST(NULL AS {}) as \"{}\"", sql_type, field.name()));
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
                    projections.push(format!("CAST(\"{}\" AS {}) as \"{}\"", field.name(), sql_type, field.name()));
                } else {
                    projections.push(format!("CAST(NULL AS {}) as \"{}\"", sql_type, field.name()));
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
            description: "Clean semantic inconsistencies using AI-generated rules based on a data sample.".to_string(),
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

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let column = cfg::required_string(config, "column")?;
        let rules = config["rules"].as_object();
        
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        
        if let Some(r) = rules {
            let mut case_parts = Vec::new();
            for (incorrect, correct) in r {
                case_parts.push(format!("WHEN \"{}\" = '{}' THEN '{}'", column, incorrect, correct.as_str().unwrap_or("")));
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
            description: "Inspect data profile and dynamically route to downstream branches.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to route".into(),
            }],
            output_ports: vec![
                PortSpec { id: 0, name: "branch_a".into(), description: "Primary branch".into() },
                PortSpec { id: 1, name: "branch_b".into(), description: "Secondary branch".into() },
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

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
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

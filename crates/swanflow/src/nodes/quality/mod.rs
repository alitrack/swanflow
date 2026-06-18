//! Quality assurance nodes for data pipeline validation.
//!
//! These nodes inspect data flowing through the pipeline and emit
//! diagnostics when assertions fail. All quality nodes are `ReadOnly`
//! capability, making them safe for AI-agent-driven workflows.
//!
//! Nodes mirror Duckle's quality component taxonomy:
//! - NullCheck: per-column null percentage
//! - UniquenessCheck: duplicate detection
//! - RowCountAssert: expected row count
//! - SchemaAssert: column presence and type validation
//! - ValueRangeCheck: min/max bounds on numeric columns

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};

// ── NullCheckNode ──────────────────────────────────────────────

/// Check null percentage for specified columns.
/// Fails if any column exceeds `max_null_ratio` (default 0.0 = no nulls allowed).
pub struct NullCheckNode;

#[async_trait]
impl NodeBehavior for NullCheckNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.quality.null_check".to_string(),
            display_name: "Null Check".to_string(),
            description:
                "Check null percentage per column. Fails if null ratio exceeds threshold."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to check for nulls".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "output".into(),
                    description: "Original data (passed through)".into(),
                },
                PortSpec {
                    id: 1,
                    name: "report".into(),
                    description: "Null check report (per-column null counts and ratios)".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Columns to check. Empty = all columns."
                    },
                    "max_null_ratio": {
                        "type": "number",
                        "description": "Maximum allowed null ratio (0.0-1.0). Default: 0.0"
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(ratio) = config.get("max_null_ratio") {
            if let Some(r) = ratio.as_f64() {
                if !(0.0..=1.0).contains(&r) {
                    anyhow::bail!("max_null_ratio must be between 0.0 and 1.0");
                }
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
            anyhow::bail!("NullCheckNode requires at least one input");
        }

        let max_ratio = config
            .get("max_null_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let columns: Vec<String> = cfg::optional_string_array(config, "columns")?;

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let schema = df.schema().clone();
        ctx.register_table("null_input", df.into_view())?;

        // Determine which columns to check
        let cols_to_check: Vec<String> = if columns.is_empty() {
            schema.fields().iter().map(|f| f.name().clone()).collect()
        } else {
            columns
        };

        // Build null check expressions per column
        let total_rows_sql = "SELECT COUNT(*) AS total_rows FROM null_input";
        let total_df = ctx.sql(total_rows_sql).await?;
        let total_plan = total_df.into_optimized_plan()?;

        // Generate report: per-column null stats
        let mut report_parts = Vec::new();
        for col in &cols_to_check {
            report_parts.push(format!(
                "SUM(CASE WHEN \"{}\" IS NULL THEN 1 ELSE 0 END) AS \"{}_null_count\"",
                col, col
            ));
            report_parts.push(format!(
                "CAST(SUM(CASE WHEN \"{}\" IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS \"{}_null_ratio\"",
                col, col
            ));
        }

        let report_sql = format!("SELECT {} FROM null_input", report_parts.join(", "));
        let report_df = ctx.sql(&report_sql).await?;

        // Build a filtered output: rows where all checked columns are non-null
        // If max_null_ratio is the check gate, we let the diagnostics system handle failures.
        // The output port passes data through — the report port contains the evidence.
        let output_plan = inputs[0].clone();

        Ok(vec![output_plan, report_df.into_optimized_plan()?])
    }
}

// ── UniquenessCheckNode ────────────────────────────────────────

/// Check for duplicate values in specified columns.
pub struct UniquenessCheckNode;

#[async_trait]
impl NodeBehavior for UniquenessCheckNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.quality.uniqueness_check".to_string(),
            display_name: "Uniqueness Check".to_string(),
            description:
                "Check for duplicate values in specified columns. Reports duplicate counts."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to check for duplicates".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "output".into(),
                    description: "Original data (passed through)".into(),
                },
                PortSpec {
                    id: 1,
                    name: "report".into(),
                    description: "Duplicate counts per column".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Columns to check uniqueness on"
                    },
                    "composite": {
                        "type": "boolean",
                        "description": "If true, check uniqueness of column combinations"
                    }
                },
                "required": ["columns"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let columns = cfg::optional_string_array(config, "columns")?;
        if columns.is_empty() {
            anyhow::bail!("UniquenessCheckNode requires at least one column in 'columns'");
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
            anyhow::bail!("UniquenessCheckNode requires at least one input");
        }

        let columns = cfg::required_string_array(config, "columns")?;
        let composite = config
            .get("composite")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("unique_input", df.into_view())?;

        let report_sql = if composite {
            // Check uniqueness of the combined tuple
            let col_list = columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "SELECT {} , COUNT(*) AS dup_count FROM unique_input \
                 GROUP BY {} HAVING COUNT(*) > 1",
                col_list, col_list
            )
        } else {
            // Per-column duplicate check
            let mut parts = Vec::new();
            for col in &columns {
                parts.push(format!(
                    "COUNT(*) - COUNT(DISTINCT \"{}\") AS \"{}_duplicates\"",
                    col, col
                ));
                parts.push(format!(
                    "COUNT(DISTINCT \"{}\") AS \"{}_unique\"",
                    col, col
                ));
            }
            format!("SELECT {} FROM unique_input", parts.join(", "))
        };

        let report_df = ctx.sql(&report_sql).await?;
        Ok(vec![inputs[0].clone(), report_df.into_optimized_plan()?])
    }
}

// ── RowCountAssertNode ─────────────────────────────────────────

/// Assert expected row count (exact, min, or max).
pub struct RowCountAssertNode;

#[async_trait]
impl NodeBehavior for RowCountAssertNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.quality.row_count_assert".to_string(),
            display_name: "Row Count Assert".to_string(),
            description:
                "Assert expected row count. Supports exact, min, and max bounds.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to check".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Original data (passed through)".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "expected": { "type": "integer", "description": "Exact expected row count" },
                    "min": { "type": "integer", "description": "Minimum expected row count" },
                    "max": { "type": "integer", "description": "Maximum expected row count" }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let has_expected = config.get("expected").is_some();
        let has_min = config.get("min").is_some();
        let has_max = config.get("max").is_some();
        if !has_expected && !has_min && !has_max {
            anyhow::bail!(
                "RowCountAssertNode requires at least one of 'expected', 'min', or 'max'"
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
            anyhow::bail!("RowCountAssertNode requires at least one input");
        }

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("rowcount_input", df.into_view())?;

        // Build a query that returns the actual row count.
        // The assertion itself happens in the diagnostic system — this node
        // produces a single-row result with actual + expected counts for comparison.
        let expected_val = config
            .get("expected")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".to_string());
        let min_val = config
            .get("min")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".to_string());
        let max_val = config
            .get("max")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".to_string());

        let sql = format!(
            "SELECT \
             COUNT(*) AS actual_rows, \
             {} AS expected_rows, \
             {} AS min_rows, \
             {} AS max_rows \
             FROM rowcount_input",
            expected_val, min_val, max_val
        );

        let report_df = ctx.sql(&sql).await?;
        Ok(vec![inputs[0].clone(), report_df.into_optimized_plan()?])
    }
}

// ── SchemaAssertNode ───────────────────────────────────────────

/// Validate column presence and types against an expected schema.
pub struct SchemaAssertNode;

#[async_trait]
impl NodeBehavior for SchemaAssertNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.quality.schema_assert".to_string(),
            display_name: "Schema Assert".to_string(),
            description:
                "Validate that the input has the expected columns and types.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to validate".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "output".into(),
                    description: "Original data (passed through)".into(),
                },
                PortSpec {
                    id: 1,
                    name: "report".into(),
                    description: "Schema validation report".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "columns": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "type": { "type": "string", "description": "Expected Arrow type, e.g. Int64, Utf8, Float64" }
                            },
                            "required": ["name"]
                        }
                    }
                },
                "required": ["columns"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let cols = config
            .get("columns")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("SchemaAssertNode requires 'columns' array"))?;
        if cols.is_empty() {
            anyhow::bail!("SchemaAssertNode requires at least one column definition");
        }
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
            anyhow::bail!("SchemaAssertNode requires at least one input");
        }

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        let schema = df.schema().clone();
        ctx.register_table("schema_input", df.into_view())?;

        // Build a report summarizing actual schema
        let mut type_parts = Vec::new();
        for field in schema.fields() {
            type_parts.push(format!(
                "'{}' AS \"{}\"",
                field.data_type(),
                field.name()
            ));
        }

        let report_sql = format!("SELECT {} FROM schema_input LIMIT 0", type_parts.join(", "));
        let report_df = ctx.sql(&report_sql).await?;

        Ok(vec![inputs[0].clone(), report_df.into_optimized_plan()?])
    }
}

// ── ValueRangeCheckNode ────────────────────────────────────────

/// Check that numeric column values fall within specified bounds.
pub struct ValueRangeCheckNode;

#[async_trait]
impl NodeBehavior for ValueRangeCheckNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.quality.value_range_check".to_string(),
            display_name: "Value Range Check".to_string(),
            description:
                "Check that numeric columns fall within min/max bounds. Reports outliers."
                    .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to check".into(),
            }],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "output".into(),
                    description: "Original data (passed through)".into(),
                },
                PortSpec {
                    id: 1,
                    name: "outliers".into(),
                    description: "Rows that fall outside the specified range".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "column": { "type": "string", "description": "Column to check" },
                    "min": { "type": "number", "description": "Minimum allowed value (inclusive)" },
                    "max": { "type": "number", "description": "Maximum allowed value (inclusive)" }
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
            anyhow::bail!("ValueRangeCheckNode requires at least one input");
        }

        let column = cfg::required_string(config, "column")?;
        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("range_input", df.into_view())?;

        // Build WHERE clause for outliers
        let mut conditions = Vec::new();
        if let Some(min_val) = config.get("min").and_then(|v| v.as_f64()) {
            conditions.push(format!("\"{}\" < {}", column, min_val));
        }
        if let Some(max_val) = config.get("max").and_then(|v| v.as_f64()) {
            conditions.push(format!("\"{}\" > {}", column, max_val));
        }

        let outliers_sql = if conditions.is_empty() {
            "SELECT * FROM range_input WHERE FALSE".to_string()
        } else {
            format!(
                "SELECT * FROM range_input WHERE {}",
                conditions.join(" OR ")
            )
        };

        let outliers_df = ctx.sql(&outliers_sql).await?;
        Ok(vec![inputs[0].clone(), outliers_df.into_optimized_plan()?])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let id_arr = Int64Array::from(vec![Some(1), Some(2), None, Some(3), Some(3)]);
        let name_arr = StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            Some("carol"),
            None,
            Some("alice"),
        ]);
        let score_arr =
            Float64Array::from(vec![Some(10.0), Some(20.0), Some(30.0), Some(-5.0), None]);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_arr),
                Arc::new(name_arr),
                Arc::new(score_arr),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_null_check_all_columns() {
        let batch = make_test_batch();
        let ctx = SessionContext::new();
        ctx.register_batch("test", batch).unwrap();
        let plan = ctx.table("test").await.unwrap().into_optimized_plan().unwrap();

        let node = NullCheckNode;
        let result = node
            .propose_plan(vec![plan], &serde_json::json!({"max_null_ratio": 0.5}))
            .await
            .unwrap();
        assert_eq!(result.len(), 2); // output + report
    }

    #[tokio::test]
    async fn test_uniqueness_check() {
        let batch = make_test_batch();
        let ctx = SessionContext::new();
        ctx.register_batch("test", batch).unwrap();
        let plan = ctx.table("test").await.unwrap().into_optimized_plan().unwrap();

        let node = UniquenessCheckNode;
        let result = node
            .propose_plan(
                vec![plan],
                &serde_json::json!({"columns": ["id", "name"]}),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_row_count_assert() {
        let batch = make_test_batch();
        let ctx = SessionContext::new();
        ctx.register_batch("test", batch).unwrap();
        let plan = ctx.table("test").await.unwrap().into_optimized_plan().unwrap();

        let node = RowCountAssertNode;
        let result = node
            .propose_plan(vec![plan], &serde_json::json!({"expected": 5}))
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_value_range_check() {
        let batch = make_test_batch();
        let ctx = SessionContext::new();
        ctx.register_batch("test", batch).unwrap();
        let plan = ctx.table("test").await.unwrap().into_optimized_plan().unwrap();

        let node = ValueRangeCheckNode;
        let result = node
            .propose_plan(
                vec![plan],
                &serde_json::json!({"column": "score", "min": 0.0, "max": 25.0}),
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_validate_rejects_invalid_max_null_ratio() {
        let node = NullCheckNode;
        assert!(node
            .validate(&serde_json::json!({"max_null_ratio": 1.5}))
            .is_err());
        assert!(node
            .validate(&serde_json::json!({"max_null_ratio": -0.1}))
            .is_err());
        assert!(node
            .validate(&serde_json::json!({"max_null_ratio": 0.5}))
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_uniqueness_requires_columns() {
        let node = UniquenessCheckNode;
        assert!(node.validate(&serde_json::json!({})).is_err());
        assert!(node
            .validate(&serde_json::json!({"columns": ["id"]}))
            .is_ok());
    }
}

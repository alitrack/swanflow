//! Built-in plan middleware: `column_select`, `row_limit`, `sample`.
//! Each is a pure DataFusion logical plan rewrite (no external engines).

use anyhow::{anyhow, Result};
use datafusion::logical_expr::{col, LogicalPlan, LogicalPlanBuilder};
use serde_json::Value as JsonValue;

use super::PlanMiddleware;

/// ── column_select ────────────────────────────────────────────────────────
/// Project only the listed columns.
pub struct ColumnSelectMiddleware {
    columns: Vec<String>,
}

pub fn column_select_builder(config: &JsonValue) -> Result<Box<dyn PlanMiddleware>> {
    let columns: Vec<String> = config
        .get("columns")
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    if columns.is_empty() {
        return Err(anyhow!(
            "column_select requires non-empty 'columns' (string array)"
        ));
    }
    Ok(Box::new(ColumnSelectMiddleware { columns }))
}

pub fn column_select_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "columns": {
                "type": "array",
                "items": { "type": "string" },
                "minItems": 1
            }
        },
        "required": ["columns"]
    })
}

impl PlanMiddleware for ColumnSelectMiddleware {
    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let exprs: Vec<_> = self.columns.iter().map(col).collect();
        LogicalPlanBuilder::from(plan.clone())
            .project(exprs)
            .and_then(|b| b.build())
            .map_err(|e| anyhow!("column_select failed: {}", e))
    }
}

/// ── row_limit ────────────────────────────────────────────────────────────
/// Cap the number of rows per input.
pub struct RowLimitMiddleware {
    limit: usize,
}

pub fn row_limit_builder(config: &JsonValue) -> Result<Box<dyn PlanMiddleware>> {
    let limit = config
        .get("limit")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("row_limit requires 'limit' (non-negative integer)"))?;
    Ok(Box::new(RowLimitMiddleware {
        limit: limit as usize,
    }))
}

pub fn row_limit_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "limit": { "type": "integer", "minimum": 0 }
        },
        "required": ["limit"]
    })
}

impl PlanMiddleware for RowLimitMiddleware {
    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(plan.clone())
            .limit(0, Some(self.limit))
            .and_then(|b| b.build())
            .map_err(|e| anyhow!("row_limit failed: {}", e))
    }
}

/// ── column_rename ─────────────────────────────────────────────────────────
/// Rename a column (pure schema operation).
pub struct ColumnRenameMiddleware {
    from: String,
    to: String,
}

pub fn column_rename_builder(config: &JsonValue) -> Result<Box<dyn PlanMiddleware>> {
    let from = config
        .get("from")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("column_rename requires 'from' (string)"))?
        .to_string();
    let to = config
        .get("to")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("column_rename requires 'to' (string)"))?
        .to_string();
    if from == to {
        return Err(anyhow!("column_rename 'from' and 'to' must differ"));
    }
    Ok(Box::new(ColumnRenameMiddleware { from, to }))
}

pub fn column_rename_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "properties": {
            "from": { "type": "string", "minLength": 1 },
            "to": { "type": "string", "minLength": 1 }
        },
        "required": ["from", "to"]
    })
}

impl PlanMiddleware for ColumnRenameMiddleware {
    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        use datafusion::logical_expr::Expr;
        use datafusion::logical_expr::expr::Alias;
        let schema = plan.schema();
        if schema.field_with_name(None, &self.from).is_err() {
            return Err(anyhow!(
                "column_rename: column '{}' not found in plan schema",
                self.from
            ));
        }
        // Project all columns, aliasing the renamed one.
        let exprs: Vec<Expr> = schema
            .fields()
            .iter()
            .map(|f| {
                let name = f.name();
                if name == &self.from {
                    Expr::Alias(Alias {
                        expr: Box::new(col(name)),
                        relation: None,
                        name: self.to.clone(),
                        metadata: None,
                    })
                } else {
                    col(name)
                }
            })
            .collect();
        LogicalPlanBuilder::from(plan.clone())
            .project(exprs)
            .and_then(|b| b.build())
            .map_err(|e| anyhow!("column_rename failed: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    use datafusion::prelude::*;
    use serde_json::json;

    fn base_plan() -> LogicalPlan {
        // A trivial plan: VALUES (1, 'a'), (2, 'b') with columns a, b.
        let df = SessionContext::new();
        let batches = datafusion::arrow::array::RecordBatch::try_from_iter(vec![
            (
                "a",
                std::sync::Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])) as _,
            ),
            (
                "b",
                std::sync::Arc::new(datafusion::arrow::array::StringArray::from(vec!["x", "y"])) as _,
            ),
        ])
        .unwrap();
        df.read_batches(vec![batches]).unwrap().into_optimized_plan().unwrap()
    }

    #[test]
    fn column_select_projects_only_listed() {
        let plan = base_plan();
        let mw = column_select_builder(&json!({"columns": ["a"]})).unwrap();
        let out = mw.apply(&plan).unwrap();
        let fields: Vec<String> = out
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, vec!["a"]);
    }

    #[test]
    fn column_select_requires_columns() {
        let err = column_select_builder(&json!({})).err().unwrap();
        assert!(err.to_string().contains("columns"));
    }

    #[test]
    fn row_limit_applies_fetch() {
        let plan = base_plan();
        let mw = row_limit_builder(&json!({"limit": 1})).unwrap();
        let out = mw.apply(&plan).unwrap();
        // Fetch should be present in the plan; we check via explain-like walk.
        let s = format!("{:?}", out);
        assert!(s.contains("fetch"), "plan should contain fetch: {}", s);
    }

    #[test]
    fn row_limit_rejects_negative() {
        let err = row_limit_builder(&json!({"limit": -5})).err().unwrap();
        assert!(err.to_string().contains("limit"));
    }

    #[test]
    fn column_rename_aliases_column() {
        let plan = base_plan();
        let mw = column_rename_builder(&json!({"from": "a", "to": "z"})).unwrap();
        let out = mw.apply(&plan).unwrap();
        let names: Vec<String> = out
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names.contains(&"z".to_string()), "renamed col present: {:?}", names);
        assert!(!names.contains(&"a".to_string()), "old name gone: {:?}", names);
    }

    #[test]
    fn column_rename_rejects_missing_column() {
        let plan = base_plan();
        let mw = column_rename_builder(&json!({"from": "nope", "to": "z"})).unwrap();
        let err = mw.apply(&plan).unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn column_rename_requires_both_names() {
        let err = column_rename_builder(&json!({"from": "a"})).err().unwrap();
        assert!(err.to_string().contains("to"));
    }
}

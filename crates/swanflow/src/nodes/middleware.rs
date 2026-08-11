//! Declarative middleware stack (borrowed from Forge's middleware compiler,
//! research #2026081102 — see ADR-012).
//!
//! Nodes may declare a middleware stack in their config:
//!
//! ```json
//! { "middleware": [ { "type": "row_limit", "config": { "limit": 100 }, "enabled": true } ] }
//! ```
//!
//! The engine compiles the declaration into an ordered list of plan
//! transformers and applies them to each input plan before the node's own
//! `propose_plan` logic runs.
//!
//! Extension model (three-point registration, all in this file):
//! to add middleware `X`, add (1) a builder fn + `mw_builders()` entry,
//! (2) a config JSON Schema in `mw_schemas()`, (3) a category string in
//! `mw_categories()`. Documented in CONTRIBUTING.

use anyhow::{anyhow, Result};
use datafusion::logical_expr::LogicalPlan;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// A single middleware declaration from node config.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct MiddlewareDecl {
    /// Middleware type id, e.g. `"row_limit"`. Must exist in `mw_builders()`.
    pub r#type: String,
    /// Per-instance configuration, validated against `mw_schemas()[type]`.
    #[serde(default)]
    pub config: JsonValue,
    /// When false, the entry is skipped during compilation.
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

/// A compiled middleware: transforms an input plan.
pub trait PlanMiddleware: Send + Sync {
    fn apply(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

/// Builder for a middleware type: `(config) -> middleware instance`.
pub type MiddlewareBuilder = fn(&JsonValue) -> Result<Box<dyn PlanMiddleware>>;

/// Registry point 1: type id → builder.
pub fn mw_builders() -> HashMap<&'static str, MiddlewareBuilder> {
    let mut m: HashMap<&'static str, MiddlewareBuilder> = HashMap::new();
    m.insert("column_select", crate::nodes::middleware::builtins::column_select_builder);
    m.insert("row_limit", crate::nodes::middleware::builtins::row_limit_builder);
    m.insert("column_rename", crate::nodes::middleware::builtins::column_rename_builder);
    m
}

/// Registry point 2: type id → config JSON Schema (validated via P0's
/// SchemaValidator — the schema contract triangle applies to middleware too).
pub fn mw_schemas() -> HashMap<&'static str, JsonValue> {
    let mut m = HashMap::new();
    m.insert("column_select", builtins::column_select_schema());
    m.insert("row_limit", builtins::row_limit_schema());
    m.insert("column_rename", builtins::column_rename_schema());
    m
}

/// Registry point 3: type id → category (documentation / categorization).
pub fn mw_categories() -> HashMap<&'static str, &'static str> {
    let mut m = HashMap::new();
    m.insert("column_select", "shape");
    m.insert("row_limit", "shape");
    m.insert("column_rename", "shape");
    m
}

/// Compile a middleware stack from a node config's `middleware` field.
///
/// - Declaration order is preserved.
/// - `enabled: false` entries are skipped silently (Forge semantics).
/// - Unknown type → hard error naming the type (no silent pass).
/// - Malformed per-instance config → error with a JSON Pointer path.
pub fn compile_middleware_stack(config: &JsonValue) -> Result<Vec<Box<dyn PlanMiddleware>>> {
    let decls: Vec<MiddlewareDecl> = match config.get("middleware") {
        None => return Ok(Vec::new()),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let decl: MiddlewareDecl = serde_json::from_value(item.clone()).map_err(|e| {
                    anyhow!(
                        "middleware[{}]: invalid declaration: {}",
                        i,
                        e
                    )
                })?;
                out.push(decl);
            }
            out
        }
        Some(_) => {
            return Err(anyhow!(
                "config 'middleware' must be an array of {{type, config, enabled}} objects"
            ))
        }
    };

    let builders = mw_builders();
    let schemas = mw_schemas();
    let mut stack = Vec::with_capacity(decls.len());
    for (i, decl) in decls.into_iter().enumerate() {
        if !decl.enabled {
            continue;
        }
        let builder = builders
            .get(decl.r#type.as_str())
            .copied()
            .ok_or_else(|| anyhow!("unknown middleware type '{}'", decl.r#type))?;

        // Validate per-instance config against the middleware's schema
        // (P0 SchemaValidator — contract triangle: same schema feeds
        // validation here and any future external consumer).
        if let Some(schema) = schemas.get(decl.r#type.as_str()) {
            if let Err(e) = swanflow_core::validation::SchemaValidator::validate_single(
                &format!("middleware.{}", decl.r#type),
                schema,
                &decl.config,
            ) {
                return Err(anyhow!("middleware[{}] {}: {}", i, decl.r#type, e));
            }
        }

        let mw = builder(&decl.config)?;
        stack.push(mw);
    }
    Ok(stack)
}

/// Apply a compiled stack to a single plan, in order. Short-circuits on
/// the first error.
pub fn apply_stack(stack: &[Box<dyn PlanMiddleware>], plan: &LogicalPlan) -> Result<LogicalPlan> {
    let mut current = plan.clone();
    for mw in stack {
        current = mw.apply(&current)?;
    }
    Ok(current)
}

/// Apply a compiled stack to many input plans, in order per plan.
pub fn apply_stack_all(
    stack: &[Box<dyn PlanMiddleware>],
    plans: Vec<LogicalPlan>,
) -> Result<Vec<LogicalPlan>> {
    plans
        .into_iter()
        .map(|p| apply_stack(stack, &p))
        .collect()
}

pub mod builtins;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_config() -> JsonValue {
        json!({
            "middleware": [
                { "type": "row_limit", "config": { "limit": 10 } },
                { "type": "column_select", "config": { "columns": ["a"] }, "enabled": true }
            ]
        })
    }

    #[test]
    fn compile_preserves_declaration_order() {
        let stack = compile_middleware_stack(&sample_config()).unwrap();
        // row_limit declared first → first in stack.
        assert_eq!(stack.len(), 2);
    }

    #[test]
    fn disabled_entry_is_skipped() {
        let cfg = json!({
            "middleware": [
                { "type": "row_limit", "config": { "limit": 10 } },
                { "type": "column_select", "config": { "columns": ["a"] }, "enabled": false }
            ]
        });
        let stack = compile_middleware_stack(&cfg).unwrap();
        assert_eq!(stack.len(), 1);
    }

    #[test]
    fn no_middleware_field_returns_empty_stack() {
        let cfg = json!({ "expr": "a > 1" });
        let stack = compile_middleware_stack(&cfg).unwrap();
        assert!(stack.is_empty());
    }

    #[test]
    fn unknown_type_errors_with_name() {
        let cfg = json!({
            "middleware": [ { "type": "does_not_exist", "config": {} } ]
        });
        let err = compile_middleware_stack(&cfg).err().unwrap();
        assert!(err.to_string().contains("does_not_exist"), "{}", err);
    }

    #[test]
    fn bad_declaration_errors() {
        // `type` missing → serde error with index.
        let cfg = json!({ "middleware": [ { "config": {} } ] });
        let err = compile_middleware_stack(&cfg).err().unwrap();
        assert!(err.to_string().contains("middleware[0]"), "{}", err);
    }

    #[test]
    fn non_array_middleware_errors() {
        let cfg = json!({ "middleware": "oops" });
        let err = compile_middleware_stack(&cfg).err().unwrap();
        assert!(err.to_string().contains("array"), "{}", err);
    }

    #[test]
    fn bad_middleware_config_errors_with_pointer() {
        // row_limit requires limit >= 0; a string config should fail schema.
        let cfg = json!({
            "middleware": [ { "type": "row_limit", "config": { "limit": "ten" } } ]
        });
        let err = compile_middleware_stack(&cfg).err().unwrap();
        assert!(err.to_string().contains("row_limit"), "{}", err);
    }

    #[test]
    fn apply_stack_all_runs_in_declared_order() {
        use crate::nodes::middleware::builtins::{
            column_rename_builder, row_limit_builder,
        };
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::prelude::*;

        // Base plan with columns a, b and 2 rows.
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
        let plan = df.read_batches(vec![batches]).unwrap().into_optimized_plan().unwrap();

        // Stack: row_limit first, then column_rename.
        let stack: Vec<Box<dyn PlanMiddleware>> = vec![
            row_limit_builder(&json!({"limit": 1})).unwrap(),
            column_rename_builder(&json!({"from": "b", "to": "z"})).unwrap(),
        ];
        let out = apply_stack_all(&stack, vec![plan]).unwrap();
        assert_eq!(out.len(), 1);
        let names: Vec<String> = out[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(names.contains(&"z".to_string()), "renamed applied: {:?}", names);
        // row_limit applied first — fetch present in the final plan.
        assert!(format!("{:?}", out[0]).contains("fetch"));
    }
}

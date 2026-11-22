//! The base relational vocabulary: sort, limit, distinct, cast, union, join.
//!
//! These six operators had no `type_id` at all. The engine could reach them by
//! writing SQL in `core.sql`, but a visual DAG whose palette has "Aggregate"
//! and "Pivot" and no "Sort" or "Join" is not usable for ordinary work — and
//! `plugin-architecture.md` §1.2 lists exactly this gap.
//!
//! ## Why they live in a separate module
//!
//! They are written *plugin-style*, as §P2 of that document requires: each one
//! depends only on the [`NodeBehavior`] contract and DataFusion, never on engine
//! internals. Moving any of them into a plugin later costs nothing, which is the
//! point — the lightness goal must not be paid for twice.
//!
//! ## Statement about input
//!
//! Identifier-bearing config (`columns`, `by[].column`, `casts` keys) goes
//! through [`quote_ident`], so a column name cannot inject SQL. The free-form
//! fields (`on`, and cast *types* which are whitelisted) are the same trust
//! level as `core.transform.filter`'s `expr`: they are the user's own SQL in
//! their own workflow, not data.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

use crate::core::engine::duckdb_backend::quote_ident;
use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec, TypedNodeBehavior};

/// Register `input` as a view named `alias` in a throwaway context, run `sql`,
/// and hand back the plan.
///
/// Every node below is the same three steps, so the shape is factored out
/// rather than copied six times. The returned plan carries its table provider,
/// so it executes on the engine's context, not this one.
async fn sql_plan(alias: &str, input: LogicalPlan, sql: &str) -> Result<LogicalPlan> {
    let ctx = SessionContext::new();
    let df = DataFrame::new(ctx.state(), input);
    ctx.register_table(alias, df.into_view())?;
    let out = ctx.sql(sql).await?;
    Ok(out.into_optimized_plan()?)
}

fn one_input<'a>(node: &str, inputs: &'a [LogicalPlan]) -> Result<&'a LogicalPlan> {
    if inputs.is_empty() {
        return Err(anyhow!("{node} requires at least one input"));
    }
    Ok(&inputs[0])
}

// ---------------------------------------------------------------------------
// Sort
// ---------------------------------------------------------------------------

/// Sort rows by one or more keys.
///
/// `by` accepts either a bare column name or `{column, desc, nulls}`. Mixed
/// directions matter in practice (`ORDER BY a ASC, b DESC`), so the per-key
/// form is the primary one and the bare string is shorthand for
/// `{column: <s>}`.
pub struct SortNode;

#[async_trait]
impl NodeBehavior for SortNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.sort".to_string(),
            display_name: "Sort".to_string(),
            description: "Order rows by one or more columns.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Rows to order".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Ordered rows".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "by": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "oneOf": [
                                { "type": "string" },
                                {
                                    "type": "object",
                                    "properties": {
                                        "column": { "type": "string" },
                                        "desc": { "type": "boolean" },
                                        "nulls": { "enum": ["first", "last"] }
                                    },
                                    "required": ["column"]
                                }
                            ]
                        }
                    }
                },
                "required": ["by"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let by = cfg::required_array(config, "by")?;
        if by.is_empty() {
            return Err(anyhow!("Sort 'by' must name at least one column"));
        }
        for (i, key) in by.iter().enumerate() {
            match key {
                JsonValue::String(s) if !s.trim().is_empty() => {}
                JsonValue::Object(o) => {
                    let col = o.get("column").and_then(|v| v.as_str()).unwrap_or("");
                    if col.trim().is_empty() {
                        return Err(anyhow!("Sort 'by[{i}].column' must be a column name"));
                    }
                    if let Some(n) = o.get("nulls") {
                        match n.as_str() {
                            Some("first") | Some("last") => {}
                            _ => {
                                return Err(anyhow!(
                                    "Sort 'by[{i}].nulls' must be \"first\" or \"last\""
                                ))
                            }
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "Sort 'by[{i}]' must be a column name or an object with a 'column'"
                    ))
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
        let input = one_input("SortNode", &inputs)?.clone();
        let by = cfg::required_array(config, "by")?;

        let mut terms = Vec::with_capacity(by.len());
        for key in by {
            let (column, desc, nulls) = match key {
                JsonValue::String(s) => (s.clone(), false, None),
                JsonValue::Object(o) => (
                    o.get("column")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    o.get("desc").and_then(|v| v.as_bool()).unwrap_or(false),
                    o.get("nulls")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                ),
                _ => {
                    return Err(anyhow!(
                        "Sort 'by' entries must be a column name or an object with a 'column'"
                    ))
                }
            };
            let mut term = format!(
                "{} {}",
                quote_ident(&column),
                if desc { "DESC" } else { "ASC" }
            );
            if let Some(n) = nulls {
                term.push_str(" NULLS ");
                term.push_str(&n.to_uppercase());
            }
            terms.push(term);
        }

        let sql = format!("SELECT * FROM sort_input ORDER BY {}", terms.join(", "));
        Ok(vec![sql_plan("sort_input", input, &sql).await?])
    }
}

impl TypedNodeBehavior for SortNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

// ---------------------------------------------------------------------------
// Limit
// ---------------------------------------------------------------------------

/// Keep at most `limit` rows, optionally skipping `offset` first.
pub struct LimitNode;

#[async_trait]
impl NodeBehavior for LimitNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.limit".to_string(),
            display_name: "Limit".to_string(),
            description: "Keep the first N rows, optionally after skipping an offset.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Rows to trim".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "At most N rows".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "minimum": 0 },
                    "offset": { "type": "integer", "minimum": 0 }
                },
                "required": ["limit"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let _ = count(config, "limit", true)?;
        let _ = count(config, "offset", false)?;
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
        let input = one_input("LimitNode", &inputs)?.clone();
        // LIMIT takes a literal, so these must be validated integers — a
        // non-integer would either fail downstream or silently round.
        let limit = count(config, "limit", true)?.unwrap_or(0);
        let offset = count(config, "offset", false)?.unwrap_or(0);
        let mut sql = format!("SELECT * FROM limit_input LIMIT {limit}");
        if offset > 0 {
            sql.push_str(&format!(" OFFSET {offset}"));
        }
        Ok(vec![sql_plan("limit_input", input, &sql).await?])
    }
}

/// Read a non-negative integer row count. `f64` is what the JSON layer gives;
/// truncating it silently would turn 2.5 into 2, so a fraction is an error.
fn count(config: &JsonValue, key: &str, required: bool) -> Result<Option<u64>> {
    let raw = if required {
        Some(cfg::required_number(config, key)?)
    } else {
        cfg::optional_number(config, key)
    };
    let Some(v) = raw else { return Ok(None) };
    if !v.is_finite() || v < 0.0 || v.fract() != 0.0 {
        return Err(anyhow!(
            "'{key}' must be a non-negative whole number, got {v}"
        ));
    }
    Ok(Some(v as u64))
}

impl TypedNodeBehavior for LimitNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

// ---------------------------------------------------------------------------
// Distinct
// ---------------------------------------------------------------------------

/// Remove duplicate rows, optionally considering only some columns.
pub struct DistinctNode;

#[async_trait]
impl NodeBehavior for DistinctNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.distinct".to_string(),
            display_name: "Distinct".to_string(),
            description: "Drop duplicate rows. Without 'columns' the whole row is compared."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Rows to de-duplicate".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "De-duplicated rows".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "columns": { "type": "array", "items": { "type": "string" } }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        // `optional_string_array` cannot tell "absent" from "present but
        // empty" — both give an empty Vec — so inspect the raw value to reject
        // the one case that is a config mistake rather than a default.
        let cols = cfg::optional_string_array(config, "columns")?;
        if config.get("columns").is_some() && cols.is_empty() {
            return Err(anyhow!(
                "Distinct 'columns' is empty; omit it entirely to compare whole rows"
            ));
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
        let input = one_input("DistinctNode", &inputs)?.clone();
        let cols = cfg::optional_string_array(config, "columns")?;
        let projection = if cols.is_empty() {
            "*".to_string()
        } else {
            cols.iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let sql = format!("SELECT DISTINCT {projection} FROM distinct_input");
        Ok(vec![sql_plan("distinct_input", input, &sql).await?])
    }
}

impl TypedNodeBehavior for DistinctNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

// ---------------------------------------------------------------------------
// Cast
// ---------------------------------------------------------------------------

/// Change column types without dropping or reordering anything else.
///
/// The projection is built from the *input schema* rather than `SELECT *
/// EXCLUDE`, because `EXCLUDE` is DuckDB syntax: relying on it would tie this
/// node to one backend, and infer_schema() has no schema to give EXCLUDE anyway.
pub struct CastNode;

impl CastNode {
    /// Whitelist: a type string reaches the SQL text, so an unknown type is
    /// rejected here rather than passed through.
    fn sql_type(raw: &str) -> Result<String> {
        let t = raw.trim().to_ascii_lowercase();
        // `decimal(p,s)` is the only parameterised form we accept.
        if let Some(rest) = t.strip_prefix("decimal") {
            let rest = rest.trim();
            if rest.is_empty() {
                return Ok("DECIMAL".to_string());
            }
            let inner = rest
                .strip_prefix('(')
                .and_then(|r| r.strip_suffix(')'))
                .ok_or_else(|| anyhow!("'{raw}' is not a valid type; use decimal(p,s)"))?;
            let (p, s) = match inner.split_once(',') {
                Some((p, s)) => (p.trim(), Some(s.trim())),
                None => (inner.trim(), None),
            };
            let p: u32 = p
                .parse()
                .map_err(|_| anyhow!("decimal precision in '{raw}' is not a number"))?;
            if p == 0 || p > 38 {
                return Err(anyhow!("decimal precision must be 1..=38, got {p}"));
            }
            return match s {
                None => Ok(format!("DECIMAL({p})")),
                Some(s) => {
                    let s: u32 = s
                        .parse()
                        .map_err(|_| anyhow!("decimal scale in '{raw}' is not a number"))?;
                    if s > p {
                        return Err(anyhow!("decimal scale {s} exceeds precision {p}"));
                    }
                    Ok(format!("DECIMAL({p},{s})"))
                }
            };
        }
        let mapped = match t.as_str() {
            "int" | "integer" => "INTEGER",
            "bigint" | "long" => "BIGINT",
            "smallint" | "short" => "SMALLINT",
            "tinyint" | "byte" => "TINYINT",
            "double" | "float64" => "DOUBLE",
            "real" | "float" | "float32" => "REAL",
            "varchar" | "string" | "text" => "VARCHAR",
            "boolean" | "bool" => "BOOLEAN",
            "date" => "DATE",
            "timestamp" | "datetime" => "TIMESTAMP",
            "time" => "TIME",
            _ => {
                return Err(anyhow!(
                    "'{raw}' is not a supported target type (try int, bigint, double, \
                     varchar, boolean, date, timestamp, decimal(p,s))"
                ))
            }
        };
        Ok(mapped.to_string())
    }
}

#[async_trait]
impl NodeBehavior for CastNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.cast".to_string(),
            display_name: "Cast".to_string(),
            description: "Change the type of named columns; everything else passes through."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Input rows".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Same rows, retyped columns".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "casts": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "minProperties": 1
                    }
                },
                "required": ["casts"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let casts = config
            .get("casts")
            .ok_or_else(|| anyhow!("Cast requires a 'casts' object"))?
            .as_object()
            .ok_or_else(|| anyhow!("Cast 'casts' must be an object of column -> type"))?;
        if casts.is_empty() {
            return Err(anyhow!("Cast 'casts' is empty; nothing would change"));
        }
        for (column, ty) in casts {
            let ty = ty
                .as_str()
                .ok_or_else(|| anyhow!("Cast type for '{column}' must be a string"))?;
            Self::sql_type(ty)?;
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
        let input = one_input("CastNode", &inputs)?.clone();
        let casts = config
            .get("casts")
            .and_then(|v| v.as_object())
            .ok_or_else(|| anyhow!("Cast requires a 'casts' object"))?
            .clone();

        let schema = input.schema();
        let known: BTreeSet<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // Fail closed and name what exists: a typo in a column name would
        // otherwise be a silent no-op cast.
        for column in casts.keys() {
            if !known.contains(column.as_str()) {
                let mut available: Vec<&str> = known.iter().copied().collect();
                available.sort_unstable();
                return Err(anyhow!(
                    "Cast names column '{column}', which is not in the input. Available: {}",
                    available.join(", ")
                ));
            }
        }

        let mut items = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name();
            match casts.get(name) {
                Some(ty) => {
                    let ty = Self::sql_type(ty.as_str().unwrap_or_default())?;
                    items.push(format!(
                        "CAST({} AS {ty}) AS {}",
                        quote_ident(name),
                        quote_ident(name)
                    ));
                }
                None => items.push(quote_ident(name)),
            }
        }

        let sql = format!("SELECT {} FROM cast_input", items.join(", "));
        Ok(vec![sql_plan("cast_input", input, &sql).await?])
    }
}

impl TypedNodeBehavior for CastNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

// ---------------------------------------------------------------------------
// Union
// ---------------------------------------------------------------------------

/// Stack inputs vertically. `all` (default true) keeps duplicates.
pub struct UnionNode;

#[async_trait]
impl NodeBehavior for UnionNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.union".to_string(),
            display_name: "Union".to_string(),
            description: "Stack the rows of every input. Set 'all' to false to drop duplicates."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "first".into(),
                description: "First input".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "All rows stacked".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "all": { "type": "boolean", "default": true }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        // `optional_bool` yields a plain Option (no Result), and an ill-typed
        // value is already rejected by config_schema; nothing to add here.
        let _ = cfg::optional_bool(config, "all");
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
        // Two or more: a one-input union is a no-op that hides a wiring mistake
        // on the canvas.
        if inputs.len() < 2 {
            return Err(anyhow!(
                "UnionNode needs at least 2 inputs, got {}",
                inputs.len()
            ));
        }
        let all = cfg::optional_bool(config, "all").unwrap_or(true);

        let ctx = SessionContext::new();
        let mut aliases = Vec::with_capacity(inputs.len());
        for (i, plan) in inputs.into_iter().enumerate() {
            let alias = format!("union_input_{i}");
            let df = DataFrame::new(ctx.state(), plan);
            ctx.register_table(alias.as_str(), df.into_view())?;
            aliases.push(alias);
        }
        let keyword = if all { "UNION ALL" } else { "UNION" };
        let sql = aliases
            .iter()
            .map(|a| format!("SELECT * FROM {a}"))
            .collect::<Vec<_>>()
            .join(&format!(" {keyword} "));
        let out = ctx.sql(&sql).await?;
        Ok(vec![out.into_optimized_plan()?])
    }
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

/// Join two inputs on a SQL predicate.
///
/// `SELECT *` over a join produces duplicate field names, which is not a plan
/// DataFusion will build — and it is also a bad table to hand downstream. The
/// right-hand side's colliding names get a suffix, which is the same thing
/// dplyr does with `.x`/`.y`; renaming is explicit and configurable rather than
/// silent.
pub struct JoinNode;

#[async_trait]
impl NodeBehavior for JoinNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.transform.join".to_string(),
            display_name: "Join".to_string(),
            description: "Join two inputs on a SQL condition. Colliding column names from the \
                          right input get a suffix."
                .to_string(),
            input_ports: vec![
                PortSpec {
                    id: 0,
                    name: "left".into(),
                    description: "Left input".into(),
                },
                PortSpec {
                    id: 1,
                    name: "right".into(),
                    description: "Right input".into(),
                },
            ],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Joined rows".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "on": { "type": "string" },
                    "how": {
                        "enum": ["inner", "left", "right", "full"]
                    },
                    "suffix": { "type": "string" }
                },
                "required": ["on"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let on = cfg::required_string(config, "on")?;
        if on.trim().is_empty() {
            return Err(anyhow!(
                "Join 'on' must be a SQL condition, e.g. l.id = r.id"
            ));
        }
        how_keyword(cfg::optional_string(config, "how").as_deref())?;
        // An empty suffix would make the collision loop below spin forever.
        if let Some(suffix) = cfg::optional_string(config, "suffix") {
            if suffix.is_empty() {
                return Err(anyhow!(
                    "Join 'suffix' must not be empty — it is what makes colliding names unique"
                ));
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
        if inputs.len() != 2 {
            return Err(anyhow!(
                "JoinNode needs exactly 2 inputs (left, right), got {}",
                inputs.len()
            ));
        }
        let on = cfg::required_string(config, "on")?;
        let how = how_keyword(cfg::optional_string(config, "how").as_deref())?;
        let suffix = cfg::optional_string(config, "suffix").unwrap_or_else(|| "_right".into());

        let (left, right) = (inputs[0].clone(), inputs[1].clone());
        let left_fields: Vec<String> = left
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut taken: BTreeSet<String> = left_fields.iter().cloned().collect();

        let mut projection = Vec::with_capacity(left_fields.len());
        for name in &left_fields {
            projection.push(format!("l.{} AS {}", quote_ident(name), quote_ident(name)));
        }
        for field in right.schema().fields() {
            let name = field.name();
            if taken.contains(name) {
                // Keep appending until unique: a left column may already be
                // called "id_right".
                let mut candidate = format!("{name}{suffix}");
                while taken.contains(&candidate) {
                    candidate.push_str(&suffix);
                }
                projection.push(format!(
                    "r.{} AS {}",
                    quote_ident(name),
                    quote_ident(&candidate)
                ));
                taken.insert(candidate);
            } else {
                projection.push(format!("r.{} AS {}", quote_ident(name), quote_ident(name)));
                taken.insert(name.clone());
            }
        }

        let ctx = SessionContext::new();
        for (alias, plan) in [("join_left", left), ("join_right", right)] {
            let df = DataFrame::new(ctx.state(), plan);
            ctx.register_table(alias, df.into_view())?;
        }
        let sql = format!(
            "SELECT {} FROM join_left AS l {} JOIN join_right AS r ON {}",
            projection.join(", "),
            how,
            on
        );
        let out = ctx.sql(&sql).await?;
        Ok(vec![out.into_optimized_plan()?])
    }
}

fn how_keyword(raw: Option<&str>) -> Result<&'static str> {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        None | Some("inner") | Some("") => Ok("INNER"),
        Some("left") => Ok("LEFT"),
        Some("right") => Ok("RIGHT"),
        Some("full") | Some("outer") | Some("full outer") => Ok("FULL OUTER"),
        Some(other) => Err(anyhow!(
            "Join 'how' must be one of inner, left, right, full — got '{other}'"
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn orders_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "east", "west", "east", "north", "west",
                ])),
                Arc::new(Int64Array::from(vec![30, 5, 100, 5, 2])),
            ],
        )
        .unwrap();
        ctx.register_batch("orders", batch).unwrap();
        ctx
    }

    /// Rows rendered as `a|b|c`, so an assertion compares content and order
    /// rather than Arrow batch internals.
    async fn rows(ctx: &SessionContext, plan: LogicalPlan) -> Vec<String> {
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let batches = df.collect().await.unwrap();
        let mut out = Vec::new();
        for b in batches {
            for r in 0..b.num_rows() {
                let mut cells = Vec::new();
                for c in b.columns() {
                    let v = datafusion::arrow::util::display::array_value_to_string(c, r).unwrap();
                    cells.push(v);
                }
                out.push(cells.join("|"));
            }
        }
        out
    }

    async fn input(ctx: &SessionContext) -> LogicalPlan {
        ctx.table("orders")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    #[tokio::test]
    async fn sort_matches_sql_order_by() {
        let ctx = orders_ctx();
        let cfg = serde_json::json!({
            "by": [{ "column": "amount", "desc": true }, { "column": "id", "desc": false }]
        });
        let plan = SortNode
            .propose_plan(vec![input(&ctx).await], &cfg)
            .await
            .unwrap();
        let got = rows(&ctx, plan[0].clone()).await;

        let want = rows(
            &ctx,
            ctx.sql("SELECT id, region, amount FROM orders ORDER BY amount DESC, id ASC")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
        )
        .await;

        assert_eq!(got, want, "sort must match SQL ORDER BY, direction per key");
        // The input was not already in this order — otherwise the assertion is empty.
        assert_ne!(got[0], "1|east|30");
    }

    #[tokio::test]
    async fn sort_quotes_a_hostile_column_name() {
        // A column literally named `x" ; DROP TABLE orders; --` must be treated
        // as an identifier, not as SQL.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x\" ; DROP TABLE orders; --",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2, 1]))]).unwrap();
        ctx.register_batch("orders", batch).unwrap();

        let cfg = serde_json::json!({ "by": ["x\" ; DROP TABLE orders; --"] });
        let plan = SortNode
            .propose_plan(vec![input(&ctx).await], &cfg)
            .await
            .unwrap();
        let got = rows(&ctx, plan[0].clone()).await;
        assert_eq!(got, vec!["1".to_string(), "2".to_string()]);
        // and the table still exists
        assert!(ctx.table("orders").await.is_ok());
    }

    #[tokio::test]
    async fn limit_matches_sql_limit_offset() {
        let ctx = orders_ctx();
        let cfg = serde_json::json!({ "limit": 2, "offset": 1 });
        let plan = LimitNode
            .propose_plan(vec![input(&ctx).await], &cfg)
            .await
            .unwrap();
        let got = rows(&ctx, plan[0].clone()).await;
        let want = rows(
            &ctx,
            ctx.sql("SELECT * FROM orders LIMIT 2 OFFSET 1")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap(),
        )
        .await;
        assert_eq!(got, want);
        assert_eq!(got.len(), 2, "limit must actually trim the result");
    }

    #[test]
    fn limit_rejects_a_fraction_rather_than_rounding_it() {
        let cfg = serde_json::json!({ "limit": 2.5 });
        let err = LimitNode.validate(&cfg).unwrap_err().to_string();
        assert!(err.contains("whole number"), "got: {err}");
    }

    #[tokio::test]
    async fn distinct_on_all_columns_and_on_a_subset() {
        let ctx = orders_ctx();

        let all = DistinctNode
            .propose_plan(vec![input(&ctx).await], &serde_json::json!({}))
            .await
            .unwrap();
        let got_all = rows(&ctx, all[0].clone()).await;
        assert_eq!(got_all.len(), 5, "no two rows are identical");

        let on_region = DistinctNode
            .propose_plan(
                vec![input(&ctx).await],
                &serde_json::json!({ "columns": ["region"] }),
            )
            .await
            .unwrap();
        let got_regions = rows(&ctx, on_region[0].clone()).await;
        let mut sorted = got_regions.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["east".to_string(), "north".to_string(), "west".to_string()],
            "three distinct regions"
        );
    }

    #[tokio::test]
    async fn cast_retypes_named_columns_and_keeps_the_rest() {
        let ctx = orders_ctx();
        let cfg = serde_json::json!({ "casts": { "amount": "double" } });
        let plan = CastNode
            .propose_plan(vec![input(&ctx).await], &cfg)
            .await
            .unwrap();

        let schema = plan[0].schema();
        let names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
        assert_eq!(
            names,
            vec!["id", "region", "amount"],
            "column order preserved"
        );
        assert_eq!(
            schema.field_with_name(None, "amount").unwrap().data_type(),
            &DataType::Float64,
            "amount must actually be a float now"
        );
        assert_eq!(
            schema.field_with_name(None, "id").unwrap().data_type(),
            &DataType::Int64,
            "untouched columns keep their type"
        );

        let got = rows(&ctx, plan[0].clone()).await;
        assert_eq!(got.len(), 5, "cast must not drop rows");
    }

    #[test]
    fn cast_rejects_an_unknown_type_and_an_unknown_column() {
        // Type is whitelisted because it lands in the SQL text.
        let bad_type = serde_json::json!({ "casts": { "amount": "int); DROP TABLE x; --" } });
        assert!(CastNode.validate(&bad_type).is_err());

        // decimal is the one parameterised form, and its bounds are checked.
        assert!(CastNode::sql_type("decimal(10,2)").is_ok());
        assert!(CastNode::sql_type("decimal(99,2)").is_err());
        assert!(CastNode::sql_type("decimal(4,9)").is_err());
    }

    #[tokio::test]
    async fn cast_names_the_available_columns_when_one_is_unknown() {
        let ctx = orders_ctx();
        let cfg = serde_json::json!({ "casts": { "amout": "double" } });
        let err = CastNode
            .propose_plan(vec![input(&ctx).await], &cfg)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("amout"),
            "must name the offending column: {err}"
        );
        assert!(err.contains("amount"), "must list what does exist: {err}");
    }

    #[tokio::test]
    async fn union_stacks_inputs_and_can_drop_duplicates() {
        let ctx = orders_ctx();
        let plan_in = input(&ctx).await;

        let all = UnionNode
            .propose_plan(
                vec![plan_in.clone(), plan_in.clone()],
                &serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(
            rows(&ctx, all[0].clone()).await.len(),
            10,
            "UNION ALL keeps both copies"
        );

        let deduped = UnionNode
            .propose_plan(
                vec![plan_in.clone(), plan_in],
                &serde_json::json!({ "all": false }),
            )
            .await
            .unwrap();
        assert_eq!(
            rows(&ctx, deduped[0].clone()).await.len(),
            5,
            "UNION drops the copy"
        );
    }

    #[tokio::test]
    async fn union_refuses_a_single_input() {
        let ctx = orders_ctx();
        let err = UnionNode
            .propose_plan(vec![input(&ctx).await], &serde_json::json!({}))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("at least 2"), "got: {err}");
    }

    async fn regions_ctx() -> (SessionContext, LogicalPlan) {
        let ctx = orders_ctx();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("manager", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["east", "west"])),
                Arc::new(StringArray::from(vec!["ana", "bo"])),
            ],
        )
        .unwrap();
        ctx.register_batch("regions", batch).unwrap();
        let plan = ctx
            .table("regions")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        (ctx, plan)
    }

    #[tokio::test]
    async fn join_renames_only_the_colliding_column() {
        let (ctx, right) = regions_ctx().await;
        let cfg = serde_json::json!({ "on": "l.region = r.region" });
        let plan = JoinNode
            .propose_plan(vec![input(&ctx).await, right], &cfg)
            .await
            .unwrap();

        let names: Vec<String> = plan[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            names,
            vec!["id", "region", "amount", "region_right", "manager"],
            "the right 'region' collides and gets the suffix; nothing else is renamed"
        );

        let got = rows(&ctx, plan[0].clone()).await;
        // 'north' (id=4) has no row in `regions`, so an inner join drops it:
        // 4 of the 5 orders survive. `join_modes_are_honoured` is what pins the
        // outer-join behaviour; here the subject is the renaming.
        assert_eq!(got.len(), 4, "inner join drops the unmatched 'north' order");
        assert!(
            got.iter().all(|r| r.contains("east") || r.contains("west")),
            "the surviving rows carry their region through the join: {got:?}"
        );
    }

    #[tokio::test]
    async fn join_modes_are_honoured() {
        let (ctx, right) = regions_ctx().await;
        // north has no region row, so an inner join must drop it and a left join keep it.
        let left_join = JoinNode
            .propose_plan(
                vec![input(&ctx).await, right.clone()],
                &serde_json::json!({ "on": "l.region = r.region", "how": "left" }),
            )
            .await
            .unwrap();
        let left_rows = rows(&ctx, left_join[0].clone()).await;
        assert_eq!(
            left_rows.len(),
            5,
            "LEFT JOIN keeps the unmatched 'north' row"
        );

        let inner_join = JoinNode
            .propose_plan(
                vec![input(&ctx).await, right],
                &serde_json::json!({ "on": "l.region = r.region", "how": "inner" }),
            )
            .await
            .unwrap();
        assert_eq!(
            rows(&ctx, inner_join[0].clone()).await.len(),
            4,
            "INNER JOIN drops the unmatched 'north' row"
        );
    }

    #[test]
    fn join_rejects_an_unknown_mode() {
        let err = JoinNode
            .validate(&serde_json::json!({ "on": "l.id = r.id", "how": "cross" }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("inner, left, right, full"), "got: {err}");
    }

    #[test]
    fn every_node_declares_its_type_id() {
        // Cheap guard so a copy-paste slip cannot make two nodes claim one id.
        let ids = [
            SortNode.spec().type_id,
            LimitNode.spec().type_id,
            DistinctNode.spec().type_id,
            CastNode.spec().type_id,
            UnionNode.spec().type_id,
            JoinNode.spec().type_id,
        ];
        let unique: BTreeSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "duplicate type_id: {ids:?}");
        for id in &ids {
            assert!(id.starts_with("core.transform."), "unexpected id {id}");
        }
    }
}

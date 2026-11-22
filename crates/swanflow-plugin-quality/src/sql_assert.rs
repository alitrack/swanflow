// ── SqlAssertNode ──────────────────────────────────────────────
//
// Arbitrary SQL assertion, ported from duckOrch's `@test` task header
// (duck-orch a5f5579, orch_runtime/src/parser.rs). duckOrch attaches
// tests to tasks as file-header comments; SwanFlow has no task files,
// so the test becomes a first-class pipeline node.
//
// Semantics (kept from duckOrch):
//   - the assertion query runs against the input table
//   - the single-value result is compared to `expect`
//   - comparison operators: ==, !=, <, <=, >, >=
//
// Differences from duckOrch (deliberate, fail-loud fixes):
//   - duckOrch silently SKIPS a task file whose @test header fails to
//     parse; this node bails at validate() time instead.
//   - duckOrch's README shows single-quoted @test SQL which its own
//     parser rejects; here the SQL is plain JSON, no quoting trap.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;

use swanflow_core::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};

const OPS: &[(&str, &str)] = &[
    ("==", "="),
    ("!=", "!="),
    ("<=", "<="),
    (">=", ">="),
    ("<", "<"),
    (">", ">"),
];

pub struct SqlAssertNode;

#[async_trait]
impl NodeBehavior for SqlAssertNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.quality.sql_assert".to_string(),
            display_name: "SQL Assert".to_string(),
            description: "Arbitrary SQL assertion on the input table (ported from duckOrch @test). Query must return a single value; compared against `expect` with the given operator.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to assert on (queryable as `input_0`)".into(),
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
                    description: "Assertion report (actual value, expected, operator, pass/fail)".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "Assertion query against `input_0`. Must return exactly one row and one column."
                    },
                    "expect": {
                        "description": "Expected value (number or string) to compare the query result against."
                    },
                    "op": {
                        "type": "string",
                        "enum": ["==", "!=", "<", "<=", ">", ">="],
                        "description": "Comparison operator. Default: '=='"
                    }
                },
                "required": ["sql", "expect"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let sql = config
            .get("sql")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("SqlAssertNode requires 'sql' (assertion query)"))?;
        if sql.trim().is_empty() {
            anyhow::bail!("SqlAssertNode 'sql' must not be empty");
        }
        if config.get("expect").is_none() {
            anyhow::bail!("SqlAssertNode requires 'expect' (expected value)");
        }
        if let Some(op) = config.get("op").and_then(|v| v.as_str()) {
            if !OPS.iter().any(|(o, _)| *o == op) {
                anyhow::bail!(
                    "SqlAssertNode 'op' must be one of ==, !=, <, <=, >, >= (got '{}')",
                    op
                );
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
            anyhow::bail!("SqlAssertNode requires at least one input");
        }

        let sql = config
            .get("sql")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("SqlAssertNode: missing 'sql'"))?;
        let op = config
            .get("op")
            .and_then(|v| v.as_str())
            .unwrap_or("==");
        let sql_op = OPS
            .iter()
            .find(|(o, _)| *o == op)
            .map(|(_, s)| *s)
            .ok_or_else(|| anyhow!("SqlAssertNode: invalid op '{}'", op))?;

        let expect_lit = expect_literal(config)?;

        let ctx = SessionContext::new();
        let df = DataFrame::new(ctx.state(), inputs[0].clone());
        ctx.register_table("input_0", df.into_view())?;

        // Wrap the user query so the report carries actual / expected / op /
        // pass columns in one row. The pass expression mirrors duckOrch's
        // compare_value (numeric when both sides parse as numbers, else
        // string equality).
        let report_sql = format!(
            "SELECT actual, {expect_lit} AS expected, '{op}' AS op, \
             CASE \
               WHEN actual {sql_op} {expect_lit} THEN true \
               ELSE false \
             END AS pass \
             FROM ({user_sql}) AS _assert",
            sql_op = sql_op,
            expect_lit = expect_lit,
            op = op,
            user_sql = sql,
        );
        let report_df = ctx.sql(&report_sql).await?;
        Ok(vec![inputs[0].clone(), report_df.into_optimized_plan()?])
    }
}

/// Render the `expect` config value as a SQL literal.
fn expect_literal(config: &JsonValue) -> Result<String> {
    let v = config
        .get("expect")
        .ok_or_else(|| anyhow!("SqlAssertNode: missing 'expect'"))?;
    match v {
        JsonValue::Number(n) => Ok(n.to_string()),
        JsonValue::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        JsonValue::Bool(b) => Ok(b.to_string()),
        _ => anyhow::bail!("SqlAssertNode 'expect' must be a number, string, or bool"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc as StdArc;

    fn make_test_batch() -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let id_arr = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let name_arr =
            StringArray::from(vec![Some("alice"), Some("bob"), Some("carol")]);
        let score_arr =
            Float64Array::from(vec![Some(10.0), Some(20.0), Some(30.0)]);
        RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(id_arr),
                StdArc::new(name_arr),
                StdArc::new(score_arr),
            ],
        )
        .unwrap()
    }

    async fn run_report(sql: &str, expect: JsonValue, op: Option<&str>) -> RecordBatch {
        let node = SqlAssertNode;
        let mut config = serde_json::json!({ "sql": sql, "expect": expect });
        if let Some(op) = op {
            config["op"] = serde_json::json!(op);
        }
        node.validate(&config).expect("validate should pass");

        let ctx = SessionContext::new();
        let batch = make_test_batch();
        ctx.register_batch("src", batch).unwrap();
        let input_df = ctx.sql("SELECT * FROM src").await.unwrap();
        let input_plan = input_df.into_optimized_plan().unwrap();

        let outputs = node.propose_plan(vec![input_plan], &config).await.unwrap();
        assert_eq!(outputs.len(), 2, "pass-through + report");
        let ctx2 = SessionContext::new();
        let report_df = DataFrame::new(ctx2.state(), outputs[1].clone());
        report_df.collect().await.unwrap().into_iter().next().unwrap()
    }

    fn get_bool(batch: &RecordBatch, col: &str) -> bool {
        batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    }

    fn get_i64(batch: &RecordBatch, col: &str) -> i64 {
        batch
            .column(batch.schema().index_of(col).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn test_sql_assert_eq_pass() {
        let report =
            run_report("SELECT COUNT(*) AS actual FROM input_0", serde_json::json!(3), None)
                .await;
        assert!(get_bool(&report, "pass"), "count=3 expect 3 should pass");
        assert_eq!(get_i64(&report, "actual"), 3);
    }

    #[tokio::test]
    async fn test_sql_assert_negative_rows_detected() {
        // duckOrch's canonical @test shape: no negative rows allowed
        let report = run_report(
            "SELECT COUNT(*) AS actual FROM input_0 WHERE id < 0",
            serde_json::json!(0),
            None,
        )
        .await;
        assert!(get_bool(&report, "pass"));
    }

    #[tokio::test]
    async fn test_sql_assert_lte_operator() {
        let report = run_report(
            "SELECT COUNT(*) AS actual FROM input_0",
            serde_json::json!(5),
            Some("<="),
        )
        .await;
        assert!(get_bool(&report, "pass"), "3 <= 5 should pass");
    }

    #[tokio::test]
    async fn test_sql_assert_string_expect() {
        let report = run_report(
            "SELECT MAX(name) AS actual FROM input_0",
            serde_json::json!("carol"),
            None,
        )
        .await;
        assert!(get_bool(&report, "pass"), "'carol' = 'carol'");
    }

    #[tokio::test]
    async fn test_sql_assert_validate_rejects_bad_config() {
        let node = SqlAssertNode;
        // missing sql
        assert!(node.validate(&serde_json::json!({ "expect": 1 })).is_err());
        // missing expect
        assert!(node.validate(&serde_json::json!({ "sql": "SELECT 1" })).is_err());
        // bad op
        assert!(node
            .validate(&serde_json::json!({ "sql": "SELECT 1", "expect": 1, "op": "~" }))
            .is_err());
        // empty sql
        assert!(node
            .validate(&serde_json::json!({ "sql": "  ", "expect": 1 }))
            .is_err());
        // good config passes
        assert!(node
            .validate(&serde_json::json!({ "sql": "SELECT 1", "expect": 1 }))
            .is_ok());
    }

    #[tokio::test]
    async fn test_sql_assert_spec_shape() {
        let spec = SqlAssertNode.spec();
        assert_eq!(spec.type_id, "core.quality.sql_assert");
        assert_eq!(spec.input_ports.len(), 1);
        assert_eq!(spec.output_ports.len(), 2);
        assert!(matches!(SqlAssertNode.capability(), NodeCapability::ReadOnly));
    }
}

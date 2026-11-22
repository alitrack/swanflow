//! Semantic metadata extraction node.
//!
//! First stage of the semantic-compilation chain (P1, Auto-Ontology x
//! SwanFlow plan 2026-10-25): pull table/column/comment metadata from a
//! DuckDB database into an Arrow table, so downstream stages (annotate /
//! fk_infer / export) can consume catalog structure as regular dataflow.
//!
//! Mirrors Auto-Ontology's SQLDatabase contract (get_tables / get_columns
//! / get_views) expressed as one long-format table:
//!   table | column | data_type | comment | is_primary_key

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::config as cfg;
use crate::nodes::io::ossie_export::{introspect, open_duckdb_readonly};
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

pub struct SemanticExtractNode;

#[async_trait]
impl NodeBehavior for SemanticExtractNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.semantic.extract".to_string(),
            display_name: "Semantic Metadata Extract".to_string(),
            description: "Extract table/column/comment metadata from a DuckDB \
database into a long-format Arrow table (one row per column), ready for \
annotation, FK inference, or Ossie export downstream."
                .to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data — table|column|data_type|comment|is_primary_key".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "database": { "type": "string" },
                    "schema":   { "type": "string" }
                },
                "required": ["database"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let database = cfg::required_string(config, "database")?;
        if database != ":memory:" && !std::path::Path::new(&database).is_file() {
            return Err(anyhow!("DuckDB database file does not exist: {}", database));
        }
        Ok(())
    }

    fn capability(&self) -> crate::nodes::NodeCapability {
        crate::nodes::NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let database = cfg::required_string(config, "database")?;
        let schema_filter = config
            .get("schema")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let conn = open_duckdb_readonly(&database)?;
        let tables = introspect(&conn, &schema_filter)?;
        if tables.is_empty() {
            return Err(anyhow!("No base tables found in {}", database));
        }

        // long format: one row per column
        let mut table_col = Vec::new();
        let mut column_col = Vec::new();
        let mut dtype_col = Vec::new();
        let mut comment_col = Vec::new();
        let mut pk_col = Vec::new();
        for tm in &tables {
            let tname = if tm.schema.is_empty() || tm.schema == "main" {
                tm.table.clone()
            } else {
                format!("{}.{}", tm.schema, tm.table)
            };
            for cm in &tm.columns {
                table_col.push(tname.clone());
                column_col.push(cm.name.clone());
                dtype_col.push(cm.data_type.clone());
                comment_col.push(cm.comment.clone());
                pk_col.push(tm.primary_key.iter().any(|p| p == &cm.name));
            }
        }

        let schema = Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, true),
            Field::new("is_primary_key", DataType::Boolean, false),
        ]);
        let col = |vals: Vec<String>| Arc::new(StringArray::from(vals)) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                col(table_col),
                col(column_col),
                col(dtype_col),
                col(comment_col),
                Arc::new(BooleanArray::from(pk_col)),
            ],
        )
        .map_err(|e| anyhow!("Failed to build metadata batch: {}", e))?;

        let ctx = SessionContext::new();
        let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| anyhow!("MemTable: {}", e))?;
        ctx.register_table("catalog_metadata", Arc::new(mem))
            .map_err(|e| anyhow!("register: {}", e))?;
        let df = ctx
            .sql("SELECT * FROM catalog_metadata ORDER BY \"table\", \"column\"")
            .await
            .map_err(|e| anyhow!("plan: {}", e))?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for SemanticExtractNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn demo_db() -> (String, tempfile::TempDir) {
        crate::nodes::io::ossie_export::build_demo_fixture()
    }

    #[tokio::test]
    async fn extracts_long_format_metadata_with_pks() {
        let (db, _guard) = demo_db();
        let node = SemanticExtractNode;
        let plans = node
            .propose_plan(vec![], &json!({ "database": db }))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(plans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // customers(3 cols) + orders(4 cols)
        assert_eq!(rows, 7, "expected 7 columns across 2 tables");

        let mut pk_cols = Vec::new();
        for b in &batches {
            let t = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let pk = b.column(4).as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in 0..b.num_rows() {
                if pk.value(i) {
                    pk_cols.push(format!("{}.{}", t.value(i), c.value(i)));
                }
            }
        }
        assert_eq!(
            pk_cols,
            vec![
                "customers.customer_id".to_string(),
                "orders.order_id".to_string()
            ],
            "PK flags must land on the right columns"
        );
    }

    #[tokio::test]
    async fn rejects_missing_database() {
        assert!(SemanticExtractNode
            .validate(&json!({ "database": "/no/such.db" }))
            .is_err());
    }
}

//! Semantic FK inference node (deterministic probe, no LLM).
//!
//! P1 middle stage of the Auto-Ontology x SwanFlow plan (2026-10-25):
//! when a database declares no foreign keys (the common case for
//! CSV/parquet-derived DuckDB catalogs), infer join paths by probing.
//!
//! Heuristics (same family as Auto-Ontology's Validate step, but purely
//! deterministic — the LLM-ranking part stays in a later node):
//!   1. name match: a column `X` in table A whose name matches the single
//!      primary key of table B (exact, then suffix-stripped `_id`/`id`).
//!   2. probe validation: run `SELECT COUNT(*) FROM a LEFT JOIN b ON ...`
//!      on live data — the candidate must be non-empty and the FK side
//!      must not blow up the join (match_ratio within bounds).
//!
//! Output: one row per candidate with match stats, ready to feed
//! OssieExport's `relationships` via a downstream join, and directly
//! consumable as `ossie_relationship` entries in Ossie model JSON.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use duckdb::Connection;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::config as cfg;
use crate::nodes::io::ossie_export::introspect;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

struct FkCandidate {
    from_table: String,
    from_column: String,
    to_table: String,
    to_column: String,
    match_ratio: f64,
    fk_nonnull: u64,
    #[allow(dead_code)] // fk_rows kept for probe audit; match_ratio uses fk_nonnull
    fk_rows: u64,
    pk_rows: u64,
    matched_rows: u64,
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Deterministic name-match candidates: column ↔ single-column PK of
/// another table, exact name or plausible FK naming (`x_id` → `x`).
fn name_match_candidates(tables: &[crate::nodes::io::ossie_export::TableMeta]) -> Vec<(usize, String, usize, String)> {
    // (from_table_idx, from_column, to_table_idx, to_column)
    let mut out = Vec::new();
    for (ai, a) in tables.iter().enumerate() {
        for col in &a.columns {
            for (bi, b) in tables.iter().enumerate() {
                if ai == bi {
                    continue;
                }
                // only single-column PKs (composite handled later)
                if b.primary_key.len() != 1 {
                    continue;
                }
                let pk = &b.primary_key[0];
                if col.name == *pk
                    || (col.name.ends_with("_id") && col.name.trim_end_matches("_id") == pk.as_str())
                    || (col.name == format!("{}_id", pk))
                {
                    out.push((ai, col.name.clone(), bi, pk.clone()));
                }
            }
        }
    }
    out
}

/// Live-data probe: LEFT JOIN from fk side onto pk side; compute coverage.
fn probe(
    conn: &Connection,
    a_tbl: &str,
    a_col: &str,
    b_tbl: &str,
    b_col: &str,
) -> Result<(f64, u64, u64, u64, u64)> {
    let sql = format!(
        "WITH probe AS (
            SELECT a.{ac} AS fk_val, b.{bc} AS pk_val
            FROM {a} a LEFT JOIN {b} b ON a.{ac} = b.{bc}
         )
         SELECT
            COUNT(*) AS fk_rows,
            COUNT(fk_val) AS fk_nonnull,
            COUNT(pk_val) AS matched,
            (SELECT COUNT(*) FROM {b}) AS pk_rows
         FROM probe",
        a = quote_ident(a_tbl),
        ac = quote_ident(a_col),
        b = quote_ident(b_tbl),
        bc = quote_ident(b_col),
    );
    let row: (u64, u64, u64, u64) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .map_err(|e| anyhow!("FK probe failed for {}.{} → {}.{}: {}", a_tbl, a_col, b_tbl, b_col, e))?;
    let (fk_rows, fk_nonnull, matched, pk_rows) = row;
    let ratio = if fk_nonnull == 0 {
        0.0
    } else {
        matched as f64 / fk_nonnull as f64
    };
    Ok((ratio, fk_nonnull, fk_rows, pk_rows, matched))
}

/// Shared inference entry point (used by OssieExport's
/// `infer_relationships` option): name-match candidates + live probes,
/// returning Ossie-shaped relationships for validated pairs.
pub(crate) fn infer_relationships(
    conn: &Connection,
    tables: &[crate::nodes::io::ossie_export::TableMeta],
    min_ratio: f64,
) -> Result<Vec<crate::nodes::io::ossie_export::OssieRelationship>> {
    let qname = |t: &crate::nodes::io::ossie_export::TableMeta| {
        if t.schema.is_empty() || t.schema == "main" {
            t.table.clone()
        } else {
            format!("{}.{}", t.schema, t.table)
        }
    };
    let mut out = Vec::new();
    for (ai, a_col, bi, b_col) in name_match_candidates(tables) {
        let a = &tables[ai];
        let b = &tables[bi];
        let (ratio, fk_nonnull, _fk_rows, _pk_rows, _matched) =
            probe(conn, &qname(a), &a_col, &qname(b), &b_col)?;
        if fk_nonnull == 0 || ratio < min_ratio {
            continue;
        }
        out.push(crate::nodes::io::ossie_export::OssieRelationship {
            name: format!(
                "{}_to_{}",
                qname(a).replace('.', "_"),
                qname(b).replace('.', "_")
            ),
            from: qname(a),
            to: qname(b),
            from_columns: vec![a_col],
            to_columns: vec![b_col],
        });
    }
    Ok(out)
}

pub struct SemanticFkInferNode;

#[async_trait]
impl NodeBehavior for SemanticFkInferNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.semantic.fk_infer".to_string(),
            display_name: "Semantic FK Inference (probe)".to_string(),
            description: "Infer join-path relationships for a DuckDB catalog \
                          without declared FKs, via name-match candidates \
                          validated by live-data probes. Deterministic, no LLM."
                .to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data — one row per validated FK candidate".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "database": { "type": "string" },
                    "schema":   { "type": "string" },
                    "min_match_ratio": { "type": "number" },
                    "with_stats": { "type": "boolean" }
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
        let min_ratio = config
            .get("min_match_ratio")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.90);

        let conn = Connection::open(&database)
            .map_err(|e| anyhow!("Failed to open DuckDB {}: {}", database, e))?;
        let tables = introspect(&conn, &schema_filter)?;
        if tables.is_empty() {
            return Err(anyhow!("No base tables found in {}", database));
        }

        let qname = |t: &crate::nodes::io::ossie_export::TableMeta| {
            if t.schema.is_empty() || t.schema == "main" {
                t.table.clone()
            } else {
                format!("{}.{}", t.schema, t.table)
            }
        };

        let mut validated: Vec<FkCandidate> = Vec::new();
        for (ai, a_col, bi, b_col) in name_match_candidates(&tables) {
            let a = &tables[ai];
            let b = &tables[bi];
            let (ratio, fk_nonnull, fk_rows, pk_rows, matched) =
                probe(&conn, &qname(a), &a_col, &qname(b), &b_col)?;
            if fk_nonnull == 0 || ratio < min_ratio {
                continue;
            }
            validated.push(FkCandidate {
                from_table: qname(a),
                from_column: a_col,
                to_table: qname(b),
                to_column: b_col,
                match_ratio: ratio,
                fk_nonnull,
                fk_rows,
                pk_rows,
                matched_rows: matched,
            });
        }

        // Output table
        let schema = Schema::new(vec![
            Field::new("from_table", DataType::Utf8, false),
            Field::new("from_column", DataType::Utf8, false),
            Field::new("to_table", DataType::Utf8, false),
            Field::new("to_column", DataType::Utf8, false),
            Field::new("relationship_name", DataType::Utf8, false),
            Field::new("match_ratio", DataType::Float64, false),
            Field::new("fk_nonnull_rows", DataType::Int64, false),
            Field::new("matched_rows", DataType::Int64, false),
            Field::new("pk_rows", DataType::Int64, false),
            Field::new("is_validated", DataType::Boolean, false),
        ]);
        let col = |vals: Vec<String>| Arc::new(StringArray::from(vals)) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                col(validated.iter().map(|c| c.from_table.clone()).collect()),
                col(validated.iter().map(|c| c.from_column.clone()).collect()),
                col(validated.iter().map(|c| c.to_table.clone()).collect()),
                col(validated.iter().map(|c| c.to_column.clone()).collect()),
                col(validated
                    .iter()
                    .map(|c| format!("{}_to_{}", c.from_table.replace('.', "_"), c.to_table.replace('.', "_")))
                    .collect()),
                Arc::new(Float64Array::from(
                    validated.iter().map(|c| c.match_ratio).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Int64Array::from(
                    validated.iter().map(|c| c.fk_nonnull as i64).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Int64Array::from(
                    validated.iter().map(|c| c.matched_rows as i64).collect::<Vec<_>>(),
                )),
                Arc::new(datafusion::arrow::array::Int64Array::from(
                    validated.iter().map(|c| c.pk_rows as i64).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(
                    validated.iter().map(|_| true).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| anyhow!("Failed to build FK batch: {}", e))?;

        let ctx = SessionContext::new();
        let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| anyhow!("MemTable: {}", e))?;
        ctx.register_table("fk_candidates", Arc::new(mem))
            .map_err(|e| anyhow!("register: {}", e))?;
        let df = ctx
            .sql("SELECT * FROM fk_candidates ORDER BY match_ratio DESC")
            .await
            .map_err(|e| anyhow!("plan: {}", e))?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for SemanticFkInferNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn demo_db() -> String {
        // orders.customer_id → customers.customer_id is the known FK;
        // the probe must find it with ratio 1.0 (every order's customer exists).
        let p = "/tmp/ossie_rt/demo.duckdb";
        assert!(std::path::Path::new(p).is_file(), "demo fixture missing");
        p.to_string()
    }

    #[tokio::test]
    async fn infers_customer_order_fk_with_full_ratio() {
        let node = SemanticFkInferNode;
        let plans = node
            .propose_plan(vec![], &json!({ "database": demo_db() }))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(plans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows >= 1, "expected at least the orders→customers FK");
        // Column layout: from_table, from_column, to_table, to_column, name, ratio, validated
        let mut found = false;
        for b in &batches {
            let (ft, fc, tt, tc) = (
                b.column(0).as_any().downcast_ref::<StringArray>().unwrap(),
                b.column(1).as_any().downcast_ref::<StringArray>().unwrap(),
                b.column(2).as_any().downcast_ref::<StringArray>().unwrap(),
                b.column(3).as_any().downcast_ref::<StringArray>().unwrap(),
            );
            let ratio = b.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                if ft.value(i) == "orders"
                    && fc.value(i) == "customer_id"
                    && tt.value(i) == "customers"
                    && tc.value(i) == "customer_id"
                {
                    assert!((ratio.value(i) - 1.0).abs() < 1e-9, "ratio {} != 1.0", ratio.value(i));
                    found = true;
                }
            }
        }
        assert!(found, "orders.customer_id → customers.customer_id not inferred");
    }

    #[test]
    fn debug_introspect_layout() {
        let conn = crate::nodes::io::ossie_export::open_duckdb_readonly(&demo_db()).unwrap();
        let tables = introspect(&conn, "").unwrap();
        for tm in &tables {
            eprintln!(
                "table={} pk={:?} cols={:?}",
                tm.table,
                tm.primary_key,
                tm.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>()
            );
        }
        let cands = name_match_candidates(&tables);
        eprintln!("candidates: {:?}", cands);
        assert!(!cands.is_empty(), "no name-match candidates generated");
    }

    #[tokio::test]
    async fn rejects_missing_database() {
        assert!(SemanticFkInferNode
            .validate(&json!({ "database": "/no/such.db" }))
            .is_err());
    }
}

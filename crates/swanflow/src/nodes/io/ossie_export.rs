//! Ossie semantic-model exporter node.
//!
//! **Producer side** of the Ossie integration (P1, Auto-Ontology x SwanFlow
//! plan 2026-10-25): compile a DuckDB database's metadata into an Apache
//! Ossie 0.2.x semantic model file (JSON), completing the
//! OssieReadNode (consumer) ↔ OssieExportNode (producer) pair.
//!
//! Pipeline: input table (typically OssieReadNode's ossie_catalog, or any
//! table with model/dataset/name/expression-ish columns) → introspect a
//! DuckDB catalog (tables, columns, PKs, comments) → emit one
//! `semantic_model[]` entry per dataset with dialect-wrapped expressions.
//!
//! v1 scope (deterministic, no LLM): metadata extraction + default
//! row-count metrics. LLM annotation (SemanticAnnotate) and FK inference
//! (SemanticFKInfer) are separate downstream nodes.

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use duckdb::Connection;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

// ── Ossie 0.2.x output shapes ──────────────────────────────────────────

#[derive(Serialize)]
struct OssieDialectEntry {
    dialect: String,
    expression: String,
}

#[derive(Serialize)]
struct OssieExpr {
    dialects: Vec<OssieDialectEntry>,
}

impl OssieExpr {
    fn ansi(expression: impl Into<String>) -> Self {
        OssieExpr {
            dialects: vec![OssieDialectEntry {
                dialect: "ANSI_SQL".to_string(),
                expression: expression.into(),
            }],
        }
    }
}

#[derive(Serialize)]
struct OssieField {
    name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    description: String,
    expression: OssieExpr,
}

#[derive(Serialize, Default)]
struct OssieDataset {
    name: String,
    source: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    primary_key: Vec<String>,
    fields: Vec<OssieField>,
}

#[derive(Serialize)]
struct OssieMetric {
    name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    description: String,
    expression: OssieExpr,
}

#[derive(Serialize, Default)]
struct OssieSemanticModel {
    name: String,
    datasets: Vec<OssieDataset>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    metrics: Vec<OssieMetric>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relationships: Vec<JsonValue>,
}

#[derive(Serialize)]
struct OssieFile {
    version: String,
    semantic_model: Vec<OssieSemanticModel>,
}

// ── DuckDB introspection ───────────────────────────────────────────────

struct TableMeta {
    schema: String,
    table: String,
    #[allow(dead_code)] // table comment is introspected for future dataset description
    comment: String,
    columns: Vec<ColumnMeta>,
    primary_key: Vec<String>,
}

struct ColumnMeta {
    name: String,
    #[allow(dead_code)] // data_type is introspected for future typed export
    data_type: String,
    comment: String,
}

fn introspect(conn: &Connection, schema_filter: &str) -> Result<Vec<TableMeta>> {
    // Exclude DuckDB system schemas; restrict to one schema when asked.
    let mut rows = conn
        .prepare(
            "SELECT table_schema, table_name, table_type,
                    NULL as table_comment
             FROM information_schema.tables
             WHERE table_schema NOT IN ('information_schema','pg_catalog')
               AND (? = '' OR table_schema = ?)
             ORDER BY table_schema, table_name",
        )
        .map_err(|e| anyhow!("information_schema.tables query failed: {}", e))?;
    let tables: Vec<(String, String, String)> = rows
        .query_map([schema_filter, schema_filter], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .map_err(|e| anyhow!("table scan failed: {}", e))?
        .filter_map(|r| r.ok())
        .filter(|(_, _, t)| t == "BASE TABLE")
        .collect();

    let mut out = Vec::new();
    for (schema, table, _) in tables {
        // Comments live in duckdb_tables()/duckdb_columns() — fall back to
        // empty string when the entry is missing (older DBs).
        let comment: String = conn
            .query_row(
                "SELECT COALESCE(comment, '') FROM duckdb_tables()
                 WHERE schema_name = ? AND table_name = ?",
                [&schema, &table],
                |r| r.get(0),
            )
            .unwrap_or_default();

        let mut col_stmt = conn
            .prepare(
                "SELECT c.column_name, c.data_type,
                        COALESCE(cc.comment, '')
                 FROM information_schema.columns c
                 LEFT JOIN duckdb_columns() cc
                   ON cc.schema_name = c.table_schema
                  AND cc.table_name = c.table_name
                  AND cc.column_name = c.column_name
                 WHERE c.table_schema = ? AND c.table_name = ?
                 ORDER BY c.ordinal_position",
            )
            .map_err(|e| anyhow!("columns query failed: {}", e))?;
        let columns: Vec<ColumnMeta> = col_stmt
            .query_map([&schema, &table], |r| {
                Ok(ColumnMeta {
                    name: r.get(0)?,
                    data_type: r.get(1)?,
                    comment: r.get(2)?,
                })
            })
            .map_err(|e| anyhow!("column scan failed: {}", e))?
            .filter_map(|r| r.ok())
            .collect();

        // Primary key via duckdb_constraints().
        let mut pk_stmt = conn
            .prepare(
                "SELECT constraint_column_names FROM duckdb_constraints()
                 WHERE schema_name = ? AND table_name = ?
                   AND constraint_type = 'PRIMARY KEY'",
            )
            .map_err(|e| anyhow!("constraints query failed: {}", e))?;
        let raw: Option<String> = pk_stmt
            .query_map([&schema, &table], |r| r.get::<_, String>(0))
            .map_err(|e| anyhow!("pk scan failed: {}", e))?
            .filter_map(|r| r.ok())
            .next();
        let pk: Vec<String> = parse_pk_col_list(raw.as_deref());

        out.push(TableMeta {
            schema,
            table,
            comment,
            columns,
            primary_key: pk,
        });
    }
    Ok(out)
}

/// `duckdb_constraints().constraint_column_names` renders as a bracketed
/// list ("[a, b]"). Parse it back into column names.
fn parse_pk_col_list(raw: Option<&str>) -> Vec<String> {
    let s = raw.unwrap_or("").trim();
    let s = s.strip_prefix('[').unwrap_or(s);
    let s = s.strip_suffix(']').unwrap_or(s);
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// Default deterministic metric set: one row-count metric per dataset,
/// qualified per Ossie's grain rule (aggregate must reference a column).
fn default_metrics(model_name: &str, tm: &TableMeta) -> Vec<OssieMetric> {
    let col = tm
        .columns
        .first()
        .map(|c| c.name.clone())
        .unwrap_or_else(|| "*".to_string());
    vec![OssieMetric {
        name: format!("{}_rows", model_name),
        description: format!("{} 行数", tm.table),
        expression: OssieExpr::ansi(format!("COUNT({}.{})", model_name, col)),
    }]
}

fn build_model(tables: Vec<TableMeta>, model_name: &str) -> OssieFile {
    let datasets: Vec<OssieDataset> = tables
        .iter()
        .map(|tm| {
            // Ossie convention (validated against the 31 spike models):
            // dataset `name` is a short identifier; `source` is a 2-part
            // <schema>.<table> reference that `ossie_load(rebind => ...)` can
            // remap by schema prefix onto the live catalog.
            let source = if tm.schema.is_empty() {
                format!("main.{}", tm.table)
            } else {
                format!("{}.{}", tm.schema, tm.table)
            };
            OssieDataset {
                name: tm.table.clone(),
                source,
                primary_key: tm.primary_key.clone(),
                fields: tm
                    .columns
                    .iter()
                    .map(|c| OssieField {
                        name: c.name.clone(),
                        description: c.comment.clone(),
                        expression: OssieExpr::ansi(c.name.clone()),
                    })
                    .collect(),
            }
        })
        .collect();

    let metrics: Vec<OssieMetric> = tables
        .iter()
        .flat_map(|tm| default_metrics(&friendly_name(tm), tm))
        .collect();

    OssieFile {
        version: "0.2.0.dev0".to_string(),
        semantic_model: vec![OssieSemanticModel {
            name: model_name.to_string(),
            datasets,
            metrics,
            relationships: vec![],
        }],
    }
}

fn friendly_name(tm: &TableMeta) -> String {
    // Metric names must be flat identifiers; schema context lives in the
    // dataset source reference, so always use the bare table name.
    tm.table.clone()
}

pub struct OssieExportNode;

#[async_trait]
impl NodeBehavior for OssieExportNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.ossie_export".to_string(),
            display_name: "Ossie Semantic Model Export".to_string(),
            description: "Compile DuckDB catalog metadata into an Apache Ossie \
                          0.2.x semantic model (JSON). Producer counterpart of the \
                          Ossie reader node."
                .to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data — one row per exported entity".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "database": { "type": "string" },
                    "path":     { "type": "string" },
                    "schema":   { "type": "string" },
                    "model_name": { "type": "string" }
                },
                "required": ["database", "path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let database = cfg::required_string(config, "database")?;
        if database != ":memory:" && !std::path::Path::new(&database).is_file() {
            return Err(anyhow!("DuckDB database file does not exist: {}", database));
        }
        let path = cfg::required_string(config, "path")?;
        if path.is_empty() {
            return Err(anyhow!("Output path must not be empty"));
        }
        if let Some(parent) = PathBuf::from(&path).parent() {
            if !parent.as_os_str().is_empty() && !parent.is_dir() {
                return Err(anyhow!(
                    "Output directory does not exist: {}",
                    parent.display()
                ));
            }
        }
        Ok(())
    }

    fn capability(&self) -> crate::nodes::NodeCapability {
        crate::nodes::NodeCapability::ReadWrite
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let database = cfg::required_string(config, "database")?;
        let out_path = cfg::required_string(config, "path")?;
        let schema_filter = config
            .get("schema")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let model_name = config
            .get("model_name")
            .and_then(|v| v.as_str())
            .unwrap_or("swanflow_export")
            .to_string();

        let conn = Connection::open(&database)
            .map_err(|e| anyhow!("Failed to open DuckDB {}: {}", database, e))?;
        let tables = introspect(&conn, &schema_filter)?;
        if tables.is_empty() {
            return Err(anyhow!(
                "No base tables found in {} (schema filter {:?}) — nothing to export",
                database,
                schema_filter
            ));
        }
        let file = build_model(tables, &model_name);

        let text = serde_json::to_string_pretty(&file)
            .context("Failed to serialize Ossie model")?;
        std::fs::write(&out_path, &text)
            .map_err(|e| anyhow!("Failed to write {}: {}", out_path, e))?;

        // Output: a small result table describing what was exported.
        let ctx = SessionContext::new();
        let mut summary: BTreeMap<String, usize> = BTreeMap::new();
        for m in &file.semantic_model {
            summary.insert(
                "datasets".into(),
                m.datasets.len(),
            );
            summary.insert("metrics".into(), m.metrics.len());
        }
        let sql = format!(
            "SELECT '{}' AS model, {} AS datasets, {} AS metrics, '{}' AS path",
            model_name.replace('\'', "''"),
            summary.get("datasets").copied().unwrap_or(0),
            summary.get("metrics").copied().unwrap_or(0),
            out_path.replace('\'', "''")
        );
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| anyhow!("Failed to plan export summary: {}", e))?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for OssieExportNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn demo_db() -> String {
        let p = "/tmp/ossie_rt/demo.duckdb";
        if !PathBuf::from(p).is_file() {
            // Build a minimal demo database on the fly (tests that run on
            // machines without the fixture still exercise the happy path).
            let conn = Connection::open(p).unwrap();
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS t(a INTEGER PRIMARY KEY, b VARCHAR);
                 INSERT OR REPLACE INTO t VALUES (1,'x'),(2,'y');",
            )
            .unwrap();
        }
        p.to_string()
    }

    #[test]
    fn exports_valid_ossie_shape() {
        let conn = Connection::open(demo_db()).unwrap();
        let tables = introspect(&conn, "").unwrap();
        assert!(!tables.is_empty(), "demo db should have base tables");
        let file = build_model(tables, "rt_test");
        let j = serde_json::to_value(&file).unwrap();
        assert_eq!(j["version"], "0.2.0.dev0");
        let sm = &j["semantic_model"][0];
        assert!(sm["datasets"].as_array().unwrap().len() >= 1);
        // every dataset has fields and at least the row-count metric exists
        let metrics = sm["metrics"].as_array().unwrap();
        assert!(metrics.iter().any(|m| m["name"]
            .as_str()
            .unwrap()
            .ends_with("_rows")));
        // dialects wrapper present on first field expression
        let f = &sm["datasets"][0]["fields"][0]["expression"]["dialects"][0];
        assert_eq!(f["dialect"], "ANSI_SQL");
    }

    #[tokio::test]
    async fn round_trip_read_after_export() {
        // Export, then load the file with OssieReadNode — the consumer must
        // parse what the producer wrote (consumer↔producer contract).
        let db = demo_db();
        let out = "/tmp/ossie_rt/roundtrip.ossie.json";
        let exporter = OssieExportNode;
        let plans = exporter
            .propose_plan(
                vec![],
                &json!({ "database": db, "path": out, "model_name": "rt" }),
            )
            .await
            .unwrap();
        assert_eq!(plans.len(), 1);

        let reader = crate::nodes::io::ossie::OssieReadNode;
        let rplans = reader
            .propose_plan(vec![], &json!({ "path": out }))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(rplans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows >= 2, "exported model must read back with rows, got {rows}");
    }

    #[tokio::test]
    async fn rejects_missing_database() {
        let node = OssieExportNode;
        assert!(node
            .validate(&json!({ "database": "/no/such.db", "path": "/tmp/x.json" }))
            .is_err());
    }
}

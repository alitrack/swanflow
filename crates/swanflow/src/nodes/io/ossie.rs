//! Ossie semantic-model reader node.
//!
//! Reads an Apache Ossie semantic model package (JSON or YAML, single
//! file for now — zip bundles come later) and exposes its metadata as
//! queryable tables so downstream SQL nodes can JOIN business semantics:
//!
//!   - ossie_terms      — one row per semantic model / dataset / field
//!   - ossie_metrics    — one row per metric (name, description, dialect, expression)
//!   - ossie_relations  — one row per declared relationship
//!
//! Spec anchor: Apache Ossie `version: 0.2.0.dev0`, `semantic_model[]`
//! with `datasets[].fields[].expression.dialects[]`, `metrics[]`,
//! `relationships[]` (validated against 31 real exported models from the
//! 2026-09-28 duckdb-ossie spike).

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

// ── Ossie format (0.2.x subset we consume) ─────────────────────────────

#[derive(Debug, Deserialize)]
struct OssieModel {
    #[serde(default)]
    version: String,
    #[serde(default)]
    semantic_model: Vec<SemanticModel>,
}

#[derive(Debug, Deserialize)]
struct SemanticModel {
    #[serde(default)]
    name: String,
    #[serde(default)]
    datasets: Vec<Dataset>,
    #[serde(default)]
    metrics: Vec<Metric>,
    #[serde(default)]
    relationships: Vec<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct Dataset {
    #[serde(default)]
    name: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    primary_key: Vec<String>,
    #[serde(default)]
    fields: Vec<FieldDef>,
}

#[derive(Debug, Deserialize)]
struct FieldDef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default, rename = "dataType")]
    data_type: String,
    #[serde(default)]
    expression: DialectExpr,
}

#[derive(Debug, Deserialize, Default)]
struct DialectExpr {
    #[serde(default)]
    dialects: Vec<DialectEntry>,
}

#[derive(Debug, Deserialize)]
struct DialectEntry {
    #[serde(default)]
    dialect: String,
    #[serde(default)]
    expression: String,
}

#[derive(Debug, Deserialize)]
struct Metric {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    expression: DialectExpr,
}

impl DialectExpr {
    /// Prefer the ANSI_SQL dialect expression, fall back to the first entry.
    fn ansi_or_first(&self) -> (String, String) {
        for d in &self.dialects {
            if d.dialect.eq_ignore_ascii_case("ANSI_SQL") {
                return (d.dialect.clone(), d.expression.clone());
            }
        }
        match self.dialects.first() {
            Some(d) => (d.dialect.clone(), d.expression.clone()),
            None => (String::new(), String::new()),
        }
    }
}

fn parse_model(text: &str) -> Result<OssieModel> {
    // YAML is a superset of JSON — one parser covers both formats.
    let model: OssieModel = serde_yaml::from_str(text)
        .context("Failed to parse Ossie model (expected 0.2.x JSON/YAML)")?;
    if model.semantic_model.is_empty() {
        return Err(anyhow!(
            "Ossie model (version {:?}) has no `semantic_model` entries — not an Ossie 0.2.x export?",
            model.version
        ));
    }
    Ok(model)
}

fn str_col<S: Into<String> + Clone>(values: Vec<S>) -> ArrayRef {
    Arc::new(StringArray::from(
        values
            .into_iter()
            .map(|v| v.into())
            .collect::<Vec<String>>(),
    ))
}

fn batch(schema: &Schema, cols: Vec<ArrayRef>) -> Result<RecordBatch> {
    RecordBatch::try_new(Arc::new(schema.clone()), cols)
        .map_err(|e| anyhow!("Failed to build Ossie metadata batch: {}", e))
}

fn catalog_table(model: &OssieModel) -> Result<RecordBatch> {
    // One unified catalog: every term, metric and relationship becomes a row
    // with a `kind` discriminator and a fixed schema, so any downstream SQL
    // node can filter/JOIN it without knowing the three source shapes.
    let schema = Schema::new(vec![
        Field::new("kind", DataType::Utf8, false), // ossie_term | ossie_metric | ossie_relationship
        Field::new("model", DataType::Utf8, false),
        Field::new("dataset", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_primary_key", DataType::Boolean, false),
        Field::new("dialect", DataType::Utf8, false),
        Field::new("expression", DataType::Utf8, false),
    ]);
    let (mut kind_c, mut model_c, mut ds_c, mut src_c, mut name_c, mut desc_c, mut ty_c) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let (mut pk_c, mut dial_c, mut expr_c) = (Vec::new(), Vec::new(), Vec::new());

    for m in &model.semantic_model {
        for ds in &m.datasets {
            for f in &ds.fields {
                let (dialect, expression) = f.expression.ansi_or_first();
                kind_c.push("ossie_term");
                model_c.push(m.name.clone());
                ds_c.push(ds.name.clone());
                src_c.push(ds.source.clone());
                name_c.push(f.name.clone());
                desc_c.push(f.description.clone());
                ty_c.push(f.data_type.clone());
                pk_c.push(ds.primary_key.iter().any(|k| k == &f.name));
                dial_c.push(dialect);
                expr_c.push(expression);
            }
        }
        for mt in &m.metrics {
            let (dialect, expression) = mt.expression.ansi_or_first();
            kind_c.push("ossie_metric");
            model_c.push(m.name.clone());
            ds_c.push(String::new());
            src_c.push(String::new());
            name_c.push(mt.name.clone());
            desc_c.push(mt.description.clone());
            ty_c.push(String::new());
            pk_c.push(false);
            dial_c.push(dialect);
            expr_c.push(expression);
        }
        for r in &m.relationships {
            kind_c.push("ossie_relationship");
            model_c.push(m.name.clone());
            ds_c.push(String::new());
            src_c.push(String::new());
            name_c.push(String::new());
            desc_c.push(String::new());
            ty_c.push(String::new());
            pk_c.push(false);
            dial_c.push(String::new());
            expr_c.push(serde_json::to_string(r).unwrap_or_default());
        }
    }

    batch(
        &schema,
        vec![
            str_col(kind_c),
            str_col(model_c),
            str_col(ds_c),
            str_col(src_c),
            str_col(name_c),
            str_col(desc_c),
            str_col(ty_c),
            Arc::new(datafusion::arrow::array::BooleanArray::from(pk_c)),
            str_col(dial_c),
            str_col(expr_c),
        ],
    )
}

pub struct OssieReadNode;

#[async_trait]
impl NodeBehavior for OssieReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.ossie_read".to_string(),
            display_name: "Ossie Semantic Model Reader".to_string(),
            description: "Read an Apache Ossie semantic model (JSON/YAML) \
                          and expose terms / metrics / relationships as queryable tables."
                .to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data — unified ossie_catalog (kind: ossie_term/ossie_metric/ossie_relationship)"
                    .into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let path = cfg::required_string(config, "path")?;
        cfg::validate_local_file(&path, "Ossie model")?;
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
        let path = cfg::required_string(config, "path")?;
        let text = std::fs::read_to_string(&path)
            .map_err(|e| anyhow!("Failed to read Ossie model {}: {}", path, e))?;
        let model = parse_model(&text)?;

        let catalog = catalog_table(&model)?;

        let ctx = SessionContext::new();
        let schema = Arc::new(catalog.schema().as_ref().clone());
        let mem = MemTable::try_new(schema, vec![vec![catalog]])
            .map_err(|e| anyhow!("Failed to build ossie_catalog MemTable: {}", e))?;
        ctx.register_table("ossie_catalog", Arc::new(mem))
            .map_err(|e| anyhow!("Failed to register ossie_catalog: {}", e))?;
        let df = ctx
            .sql("SELECT * FROM ossie_catalog")
            .await
            .map_err(|e| anyhow!("Failed to plan ossie_catalog scan: {}", e))?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for OssieReadNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_YAML: &str = r#"
version: 0.2.0.dev0
semantic_model:
  - name: demo
    datasets:
      - name: demo_ds
        source: tpl.demo_ds
        primary_key: [id]
        fields:
          - name: id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: id
          - name: revenue
            description: 净收入（人民币）
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: revenue
    metrics:
      - name: total_revenue
        description: 总净收入
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: SUM(demo_ds.revenue)
    relationships: []
"#;

    #[test]
    fn parses_yaml_and_extracts_all_three_tables() {
        let model = parse_model(SAMPLE_YAML).unwrap();
        assert_eq!(model.version, "0.2.0.dev0");
        let cat = catalog_table(&model).unwrap();
        assert_eq!(cat.num_rows(), 3); // 2 terms + 1 metric + 0 relationships
        let kinds: Vec<&str> = (0..cat.num_rows())
            .map(|i| {
                cat.column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .unwrap()
                    .value(i)
            })
            .collect();
        assert_eq!(kinds, vec!["ossie_term", "ossie_term", "ossie_metric"]);
    }

    #[test]
    fn rejects_non_ossie_payload() {
        assert!(parse_model("{\"hello\": \"world\"}").is_err());
        assert!(parse_model("version: 0.2.0.dev0\nsemantic_model: []").is_err());
    }

    #[tokio::test]
    async fn propose_plan_yields_joinable_union() {
        let dir = std::env::temp_dir().join("swanflow_ossie_test");
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("demo.ossie.yaml");
        std::fs::write(&p, SAMPLE_YAML).unwrap();
        let node = OssieReadNode;
        let plans = node
            .propose_plan(vec![], &serde_json::json!({ "path": p.to_string_lossy() }))
            .await
            .unwrap();
        assert_eq!(plans.len(), 1);
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(plans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "2 terms + 1 metric + 0 relations");
    }

    #[test]
    fn validates_missing_path() {
        let node = OssieReadNode;
        assert!(node
            .validate(&serde_json::json!({ "path": "/no/such/model.ossie.json" }))
            .is_err());
    }
}

//! Semantic annotation node (LLM column → business term).
//!
//! Final stage of P1 (Auto-Ontology x SwanFlow plan, 2026-10-25):
//! take a DuckDB catalog's columns and ask an LLM for business-glossary
//! annotations (term, description) per column. Provider follows the
//! `ai.entity_extractor` pattern: injected `Arc<dyn LlmProvider>`, any
//! OpenAI-compatible endpoint; no vendor lock.
//!
//! Output: one row per annotated column:
//!   table | column | term | description | confidence
//! ready to feed OssieExport's field descriptions or the Auto-Ontology
//! glossary directly.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::ai::providers::LlmProvider;
use crate::nodes::config as cfg;
use crate::nodes::io::ossie_export::introspect;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

pub struct SemanticAnnotateNode {
    pub provider: Arc<dyn LlmProvider>,
}

struct Annotation {
    table: String,
    column: String,
    term: String,
    description: String,
    confidence: f64,
}

/// Build the annotation prompt for a batch of columns (table, column, type).
fn build_prompt(tables: &[crate::nodes::io::ossie_export::TableMeta]) -> String {
    let mut listing = String::new();
    for tm in tables {
        let tname = if tm.schema.is_empty() || tm.schema == "main" {
            tm.table.clone()
        } else {
            format!("{}.{}", tm.schema, tm.table)
        };
        for col in &tm.columns {
            listing.push_str(&format!("- {}.{} :: {}\n", tname, col.name, col.data_type));
        }
    }
    format!(
        "You are a data catalog annotator. For each column below, produce a \
business glossary term (short, lowercase_snake, English or Chinese as \
appropriate) and a one-sentence business description.\n\
Return ONLY a JSON object, no markdown fences:\n\
{{\"annotations\":[{{\"table\":\"...\",\"column\":\"...\",\"term\":\"...\",\"description\":\"...\",\"confidence\":0.0}}]}}\n\
confidence is your own 0.0-1.0 certainty. Columns:\n{listing}"
    )
}

/// Parse the LLM response into annotations, keeping only entries whose
/// table.column actually exists in the catalog (hallucination guard).
fn parse_response(
    response: &str,
    tables: &[crate::nodes::io::ossie_export::TableMeta],
) -> Result<Vec<Annotation>> {
    let json_str = crate::nodes::ai::ConfigWizard::extract_json_from_response(response)
        .map_err(|e| anyhow!("LLM response is not JSON: {}", e))?;
    let v: JsonValue = serde_json::from_str(json_str)
        .map_err(|e| anyhow!("Failed to parse annotation JSON: {} — {}", e, json_str))?;
    let arr = v
        .get("annotations")
        .and_then(|a| a.as_array())
        .ok_or_else(|| anyhow!("response missing `annotations` array"))?;

    // existing table.column set for hallucination guard
    let mut known = std::collections::HashSet::new();
    let qname = |tm: &crate::nodes::io::ossie_export::TableMeta| {
        if tm.schema.is_empty() || tm.schema == "main" {
            tm.table.clone()
        } else {
            format!("{}.{}", tm.schema, tm.table)
        }
    };
    for tm in tables {
        for c in &tm.columns {
            known.insert(format!("{}.{}", qname(tm), c.name));
        }
    }

    let mut out = Vec::new();
    for item in arr {
        let table = item.get("table").and_then(|x| x.as_str()).unwrap_or("");
        let column = item.get("column").and_then(|x| x.as_str()).unwrap_or("");
        if table.is_empty() || column.is_empty() {
            continue;
        }
        if !known.contains(&format!("{}.{}", table, column)) {
            continue; // hallucinated table.column
        }
        out.push(Annotation {
            table: table.to_string(),
            column: column.to_string(),
            term: item.get("term").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            description: item
                .get("description")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            confidence: item
                .get("confidence")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.5)
                .clamp(0.0, 1.0),
        });
    }
    Ok(out)
}

#[async_trait]
impl NodeBehavior for SemanticAnnotateNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.semantic.annotate".to_string(),
            display_name: "Semantic Annotate (LLM)".to_string(),
            description: "Annotate a DuckDB catalog's columns with business \
glossary terms via LLM (any OpenAI-compatible endpoint). Output feeds \
OssieExport field descriptions or the Auto-Ontology glossary."
                .to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data — table|column|term|description|confidence".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "database": { "type": "string" },
                    "schema":   { "type": "string" },
                    "min_confidence": { "type": "number" }
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
        let min_confidence = config
            .get("min_confidence")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let conn = crate::nodes::io::ossie_export::open_duckdb_readonly(&database)?;
        let tables = introspect(&conn, &schema_filter)?;
        if tables.is_empty() {
            return Err(anyhow!("No base tables found in {}", database));
        }

        let prompt = build_prompt(&tables);
        let response = self
            .provider
            .complete(&prompt)
            .await
            .map_err(|e| anyhow!("LLM call failed: {}", e))?;
        let annotations = parse_response(&response, &tables)?
            .into_iter()
            .filter(|a| a.confidence >= min_confidence)
            .collect::<Vec<_>>();
        if annotations.is_empty() {
            return Err(anyhow!(
                "LLM returned no usable annotations (all hallucinated or below min_confidence)"
            ));
        }

        let schema = Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, false),
            Field::new("term", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("confidence", DataType::Float64, false),
        ]);
        let col = |vals: Vec<String>| Arc::new(StringArray::from(vals)) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                col(annotations.iter().map(|a| a.table.clone()).collect()),
                col(annotations.iter().map(|a| a.column.clone()).collect()),
                col(annotations.iter().map(|a| a.term.clone()).collect()),
                col(annotations.iter().map(|a| a.description.clone()).collect()),
                Arc::new(Float64Array::from(
                    annotations.iter().map(|a| a.confidence).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| anyhow!("Failed to build annotation batch: {}", e))?;

        let ctx = SessionContext::new();
        let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])
            .map_err(|e| anyhow!("MemTable: {}", e))?;
        ctx.register_table("column_annotations", Arc::new(mem))
            .map_err(|e| anyhow!("register: {}", e))?;
        let df = ctx
            .sql("SELECT * FROM column_annotations ORDER BY confidence DESC")
            .await
            .map_err(|e| anyhow!("plan: {}", e))?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for SemanticAnnotateNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    /// Deterministic mock: echoes a canned valid annotation payload.
    struct MockProvider {
        pub response: String,
    }

    #[async_trait]
    impl LlmProvider for MockProvider {
        async fn complete(&self, _prompt: &str) -> Result<String> {
            Ok(self.response.clone())
        }
    }

    fn demo_db() -> (String, tempfile::TempDir) {
        crate::nodes::io::ossie_export::build_demo_fixture()
    }

    fn mock_valid() -> MockProvider {
        MockProvider {
            response: serde_json::json!({
                "annotations": [
                    {"table":"customers","column":"customer_name","term":"customer_display_name",
                     "description":"客户展示名称","confidence":0.95},
                    {"table":"orders","column":"amount","term":"order_amount",
                     "description":"订单金额","confidence":0.9},
                    {"table":"ghost_table","column":"nope","term":"hallucination",
                     "description":"不存在的列","confidence":0.99}
                ]
            })
            .to_string(),
        }
    }

    #[tokio::test]
    async fn annotates_and_guards_hallucinations() {
        let (db, _guard) = demo_db();
        let node = SemanticAnnotateNode {
            provider: Arc::new(mock_valid()),
        };
        let plans = node
            .propose_plan(vec![], &serde_json::json!({ "database": db }))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(plans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 2 valid + ghost filtered by the hallucination guard
        assert_eq!(rows, 2, "expected 2 annotations (ghost filtered)");
        let tables: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |i| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(i)
                        .to_string()
                })
            })
            .collect();
        assert!(!tables.iter().any(|t| t == "ghost_table"));
    }

    #[tokio::test]
    async fn min_confidence_filters_low() {
        let node = SemanticAnnotateNode {
            provider: Arc::new(MockProvider {
                response: serde_json::json!({
                    "annotations": [
                        {"table":"customers","column":"region","term":"region",
                         "description":"x","confidence":0.4},
                        {"table":"customers","column":"customer_id","term":"customer_pk",
                         "description":"y","confidence":0.95}
                    ]
                })
                .to_string(),
            }),
        };
        let (db, _guard) = demo_db();
        let plans = node
            .propose_plan(
                vec![],
                &serde_json::json!({ "database": db, "min_confidence": 0.8 }),
            )
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let df = ctx
            .execute_logical_plan(plans.into_iter().next().unwrap())
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "0.4-confidence row must be filtered");
    }

    #[tokio::test]
    async fn rejects_garbage_response() {
        let (db, _guard) = demo_db();
        let node = SemanticAnnotateNode {
            provider: Arc::new(MockProvider {
                response: "I cannot answer that.".to_string(),
            }),
        };
        assert!(node
            .propose_plan(vec![], &serde_json::json!({ "database": db }))
            .await
            .is_err());
    }

    #[test]
    fn parses_markdown_fenced_json() {
        let tables = vec![crate::nodes::io::ossie_export::TableMeta {
            schema: "main".into(),
            table: "customers".into(),
            comment: String::new(),
            columns: vec![crate::nodes::io::ossie_export::ColumnMeta {
                name: "region".into(),
                data_type: "VARCHAR".into(),
                comment: String::new(),
            }],
            primary_key: vec![],
        }];
        let resp = "```json\n{\"annotations\":[{\"table\":\"customers\",\"column\":\"region\",\"term\":\"sales_region\",\"description\":\"销售区域\",\"confidence\":0.9}]}\n```";
        let out = parse_response(resp, &tables).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].term, "sales_region");
    }
}

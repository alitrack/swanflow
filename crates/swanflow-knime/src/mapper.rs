use std::collections::HashMap;

use petgraph::stable_graph::NodeIndex;
use serde_json::json;
use uuid::Uuid;

use swanflow::core::graph::{EdgeInstance, NodeInstance, StepMode, Workflow};
use swanflow::nodes::{NodeCapability, NodeState};

use crate::error::{KnimeError, KnimeResult};
use crate::types::KnwfArchive;

/// Maps KNIME node types to SwanFlow native nodes.
///
/// The mapping covers the most commonly used KNIME nodes.
/// Unsupported nodes are translated into a pass-through stub or skipped
/// with a warning.
pub struct KnimeNodeMapper {
    /// Registry of known KNIME → SwanFlow mappings
    type_map: HashMap<String, SwanFlowNodeDef>,
}

/// A SwanFlow node definition for a KNIME type.
struct SwanFlowNodeDef {
    /// SwanFlow native type_id (e.g., "CsvRead", "Filter")
    pub swanflow_type: String,
    /// Whether the node is a source (no inputs)
    pub is_source: bool,
    /// Transform KNIME settings into SwanFlow node config
    pub config_fn: Box<dyn Fn(&serde_json::Value) -> serde_json::Value + Send + Sync>,
}

impl KnimeNodeMapper {
    pub fn new() -> Self {
        let mut type_map: HashMap<String, SwanFlowNodeDef> = HashMap::new();

        // ─── File IO ───
        type_map.insert(
            "org.knime.base.node.io.filehandling.CSVReaderNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "CsvRead".into(),
                is_source: true,
                config_fn: Box::new(|s| {
                    json!({
                        "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                        "has_header": s.get("has_column_header").and_then(|v| v.as_bool()).unwrap_or(true),
                        "delimiter": s.get("column_delimiter").and_then(|v| v.as_str()).unwrap_or(","),
                    })
                }),
            },
        );

        type_map.insert(
            "org.knime.base.node.io.filehandling.CSVWriterNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "CsvWrite".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    json!({
                        "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                        "delimiter": s.get("column_delimiter").and_then(|v| v.as_str()).unwrap_or(","),
                    })
                }),
            },
        );

        type_map.insert(
            "org.knime.base.node.io.filehandling.ExcelReaderNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "ExcelRead".into(),
                is_source: true,
                config_fn: Box::new(|s| {
                    json!({
                        "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                        "sheet_name": s.get("sheet_name").and_then(|v| v.as_str()),
                    })
                }),
            },
        );

        // ─── SQL / Database ───
        type_map.insert(
            "org.knime.base.node.io.database.DBReaderNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "SqlNode".into(),
                is_source: true,
                config_fn: Box::new(|s| {
                    json!({
                        "query": s.get("sql_string").and_then(|v| v.as_str()).unwrap_or(""),
                        "connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                    })
                }),
            },
        );

        // ─── Transform ───
        type_map.insert(
            "org.knime.base.node.preproc.filter.row.RowFilterNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Filter".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                    let op = s.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
                    let val = s.get("filter_value").and_then(|v| v.as_str()).unwrap_or("");
                    json!({
                        "predicate": format!("{} {} {}", col, Self::map_op(op), val)
                    })
                }),
            },
        );

        type_map.insert(
            "org.knime.base.node.preproc.filter.column.ColumnFilterNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Project".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let included = s.get("included_column_names")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                        });
                    json!({ "columns": included })
                }),
            },
        );

        type_map.insert(
            "org.knime.base.node.preproc.sort.SorterNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Sort".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                    let asc = !s.get("sort_descending")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    json!({
                        "sort_key": col,
                        "ascending": asc,
                    })
                }),
            },
        );

        // ─── Aggregation ───
        type_map.insert(
            "org.knime.base.node.preproc.groupby.GroupByNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Aggregate".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let group_cols = s.get("group_column_names")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                        });
                    let agg_cols = s.get("aggregate_column_names")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                        });
                    let agg_methods = s.get("aggregation_methods")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                        });
                    json!({
                        "group_by": group_cols,
                        "aggregates": agg_cols,
                        "methods": agg_methods,
                    })
                }),
            },
        );

        // ─── Join ───
        type_map.insert(
            "org.knime.base.node.preproc.joiner.JoinerNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Join".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let left_cols = s.get("left_column_names")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>());
                    let right_cols = s.get("right_column_names")
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>());
                    let join_type = s.get("join_type").and_then(|v| v.as_str()).unwrap_or("INNER");
                    json!({
                        "left_keys": left_cols,
                        "right_keys": right_cols,
                        "join_type": join_type,
                    })
                }),
            },
        );

        // ─── Math / Statistics ───
        type_map.insert(
            "org.knime.base.node.preproc.mathformula.MathFormulaNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Project".into(), // Math formula as expression projection
                is_source: false,
                config_fn: Box::new(|s| {
                    let col = s.get("new_column_name").and_then(|v| v.as_str()).unwrap_or("result");
                    let expr = s.get("expression").and_then(|v| v.as_str()).unwrap_or("");
                    json!({ "expressions": [{ "column": col, "formula": expr }] })
                }),
            },
        );

        type_map.insert(
            "org.knime.base.node.preproc.statistics.StatisticsNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Profiler".into(),
                is_source: false,
                config_fn: Box::new(|_| json!({})),
            },
        );

        // ─── Missing Values ───
        type_map.insert(
            "org.knime.base.node.preproc.missingval.MissingValueNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "MissingValue".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let strategy = s.get("missing_value_handling")
                        .and_then(|v| v.as_str())
                        .unwrap_or("drop");
                    json!({ "strategy": strategy })
                }),
            },
        );

        // ─── Concatenate / Append ───
        type_map.insert(
            "org.knime.base.node.preproc.append.ConcatenateNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Union".into(),
                is_source: false,
                config_fn: Box::new(|_| json!({})),
            },
        );

        // ─── Rule Engine ───
        type_map.insert(
            "org.knime.base.node.preproc.rules.RuleEngineNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "Condition".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    let rules = s.get("rules").cloned().unwrap_or(json!([]));
                    json!({ "rules": rules })
                }),
            },
        );

        // ─── Python Script ───
        type_map.insert(
            "org.knime.base.node.preproc.python.PythonScriptNodeFactory".into(),
            SwanFlowNodeDef {
                swanflow_type: "PythonNode".into(),
                is_source: false,
                config_fn: Box::new(|s| {
                    json!({
                        "script": s.get("python_script").and_then(|v| v.as_str()).unwrap_or(""),
                    })
                }),
            },
        );

        Self { type_map }
    }

    /// Convert a parsed KNIME archive into a SwanFlow Workflow.
    pub async fn convert(&self, archive: KnwfArchive) -> KnimeResult<Workflow> {
        let mut node_instances = Vec::new();
        let mut edges = Vec::new();

        // Map KNIME node IDs to petgraph NodeIndex
        let mut id_to_idx: HashMap<String, NodeIndex> = HashMap::new();

        // Convert nodes
        for knime_node in &archive.nodes {
            let idx = NodeIndex::new(node_instances.len());

            let type_id = knime_node.node_type.as_deref().unwrap_or("unknown");
            let mapped = self.type_map.get(type_id);

            let (swanflow_type, config) = if let Some(def) = &mapped {
                (def.swanflow_type.clone(), (def.config_fn)(&knime_node.settings))
            } else {
                // Unsupported node → pass-through stub
                tracing::warn!(
                    "Unknown KNIME node type: {} (id={}), treating as pass-through",
                    type_id,
                    knime_node.id
                );
                ("pass_through".to_string(), json!({}))
            };

            let instance = NodeInstance {
                id: Uuid::new_v4(),
                type_id: swanflow_type,
                config,
                state: NodeState::Idle,
                materialize: false,
                sub_workflow: None,
                mode: StepMode::Sequential,
                error_mode: swanflow::core::graph::ErrorMode::Fail,
                retry_count: 0,
            };

            id_to_idx.insert(knime_node.id.clone(), idx);
            node_instances.push(instance);
        }

        // Convert edges
        for edge in &archive.edges {
            if let (Some(&src), Some(&tgt)) = (
                id_to_idx.get(&edge.source_id),
                id_to_idx.get(&edge.target_id),
            ) {
                edges.push(EdgeInstance {
                    source_index: src,
                    source_port: edge.source_port,
                    target_index: tgt,
                    target_port: edge.target_port,
                });
            }
        }

        Ok(Workflow {
            nodes: node_instances,
            edges,
            name: "Imported KNIME Workflow".to_string(),
            version: "0.1.0".to_string(),
        })
    }

    fn map_op(knime_op: &str) -> &str {
        match knime_op.to_uppercase().as_str() {
            "EQ" | "=" => "=",
            "NEQ" | "!=" => "!=",
            "LT" | "<" => "<",
            "LE" | "<=" => "<=",
            "GT" | ">" => ">",
            "GE" | ">=" => ">=",
            "LIKE" | "CONTAINS" => "LIKE",
            "NOT_LIKE" => "NOT LIKE",
            "IS_NULL" => "IS NULL",
            "IS_NOT_NULL" => "IS NOT NULL",
            _ => knime_op,
        }
    }
}

impl Default for KnimeNodeMapper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_filter_config() {
        let mapper = KnimeNodeMapper::new();
        let csv_def = mapper.type_map.get(
            "org.knime.base.node.preproc.filter.row.RowFilterNodeFactory"
        );
        assert!(csv_def.is_some());
    }

    #[test]
    fn test_csv_reader_config() {
        let mapper = KnimeNodeMapper::new();
        let def = mapper.type_map.get(
            "org.knime.base.node.io.filehandling.CSVReaderNodeFactory"
        );
        let config = (def.unwrap().config_fn)(&json!({
            "file_name": "/data/input.csv",
            "has_column_header": true,
            "column_delimiter": ","
        }));
        assert_eq!(config["path"], "/data/input.csv");
        assert_eq!(config["has_header"], true);
    }
}

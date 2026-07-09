use std::collections::{HashMap, HashSet};

use petgraph::stable_graph::NodeIndex;
use serde_json::json;
use uuid::Uuid;

use swanflow_core::graph::{EdgeInstance, NodeInstance, StepMode, Workflow};
use swanflow_core::nodes::NodeState;

use crate::error::KnimeResult;
use crate::types::KnwfArchive;

/// Extract the KNIME factory class name from node metadata + settings JSON.
fn extract_factory(node_type: Option<&str>, settings: &serde_json::Value) -> String {
    // Try node_type first (may be the factory if set by parser)
    if let Some(nt) = node_type {
        if nt != "NativeNode" && nt != "SubNode" && nt != "MetaNode" {
            return nt.to_string();
        }
    }
    // Look in settings: model.factory is the canonical location
    if let Some(model) = settings.get("model") {
        if let Some(f) = model.get("factory").and_then(|v| v.as_str()) {
            return f.to_string();
        }
    }
    // Also try top-level factory
    if let Some(f) = settings.get("factory").and_then(|v| v.as_str()) {
        return f.to_string();
    }
    node_type.unwrap_or("unknown").to_string()
}

/// A single mapping entry for reporting/listing.
#[derive(Debug, Clone)]
pub struct MappingEntry {
    pub factory: String,
    pub sw_type: String,
    pub category: String,
    pub compat: String,
}

/// Maps KNIME node types to SwanFlow native nodes.
pub struct KnimeNodeMapper {
    type_map: HashMap<String, SwanFlowNodeDef>,
}

struct SwanFlowNodeDef {
    pub swanflow_type: String,
    pub config_fn: Box<dyn Fn(&serde_json::Value) -> serde_json::Value + Send + Sync>,
}

impl KnimeNodeMapper {
    pub fn new() -> Self {
        let mut type_map: HashMap<String, SwanFlowNodeDef> = HashMap::new();

        macro_rules! def {
            ($knime:expr, $sw_type:expr, $cfg:expr) => {
                type_map.insert(
                    $knime.into(),
                    SwanFlowNodeDef {
                        swanflow_type: $sw_type.into(),
                        config_fn: Box::new($cfg),
                    },
                );
            };
        }

        // ─── File IO ───
        def!("org.knime.base.node.io.filehandling.CSVReaderNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "has_header": s.get("has_column_header").and_then(|v| v.as_bool()).unwrap_or(true),
                       "delimiter": s.get("column_delimiter").and_then(|v| v.as_str()).unwrap_or(",")}));

        def!("org.knime.base.node.io.filehandling.CSVWriterNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "delimiter": s.get("column_delimiter").and_then(|v| v.as_str()).unwrap_or(",")}));

        def!("org.knime.base.node.io.filehandling.ExcelReaderNodeFactory", "core.io.excel_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "sheet_name": s.get("sheet_name").and_then(|v| v.as_str())}));

        // ─── Transform ───
        def!("org.knime.base.node.preproc.filter.row.RowFilterNodeFactory", "core.transform.filter",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let op = Self::map_op(s.get("operator").and_then(|v| v.as_str()).unwrap_or("="));
                let val = s.get("filter_value").and_then(|v| v.as_str()).unwrap_or("");
                json!({"expr": format!("{} {} {}", col, op, val)})
            });

        def!("org.knime.base.node.preproc.filter.column.ColumnFilterNodeFactory", "core.transform.project",
            |s| json!({"columns": s.get("included_column_names").and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())}));

        def!("org.knime.base.node.preproc.sort.SorterNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let asc = !s.get("sort_descending").and_then(|v| v.as_bool()).unwrap_or(false);
                let order = if asc { "ASC" } else { "DESC" };
                json!({"query": format!("SELECT * FROM input_0 ORDER BY {} {}", col, order)})
            });

        // ─── Aggregate ───
        def!("org.knime.base.node.preproc.groupby.GroupByNodeFactory", "core.transform.aggregate",
            |s| {
                // KNIME stores column names as comma-separated string, not JSON array
                let parse_cols = |key: &str| -> Vec<&str> {
                    s.get(key)
                        .and_then(|v| v.as_str())
                        .map(|s| s.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).collect())
                        .or_else(|| {
                            s.get(key).and_then(|v| v.as_array())
                                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                        })
                        .unwrap_or_default()
                };
                let group_cols = parse_cols("group_column_names");
                let agg_cols = parse_cols("aggregate_column_names");
                let aggs: Vec<String> = agg_cols.iter()
                    .map(|c| format!("AVG({})", c))
                    .collect();
                json!({
                    "group_by": group_cols,
                    "aggs": aggs,
                })
            });

        // ─── Join ───
        def!("org.knime.base.node.preproc.joiner.JoinerNodeFactory", "core.sql",
            |s| json!({
                "query": Self::build_join_sql(s)
            }));

        // ─── Math / Stats ───
        def!("org.knime.base.node.preproc.mathformula.MathFormulaNodeFactory", "core.transform.project",
            |s| json!({"expressions": [{"column": s.get("new_column_name")
                .and_then(|v| v.as_str()).unwrap_or("result"),
                "formula": s.get("expression").and_then(|v| v.as_str()).unwrap_or("")}]}));

        def!("org.knime.base.node.preproc.statistics.StatisticsNodeFactory", "core.transform.profiler",
            |_| json!({}));

        // ─── Missing / Concat ───
        def!("org.knime.base.node.preproc.missingval.MissingValueNodeFactory", "core.quality.null_check",
            |s| json!({"strategy": s.get("missing_value_handling")
                .and_then(|v| v.as_str()).unwrap_or("drop")}));

        def!("org.knime.base.node.preproc.append.ConcatenateNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0 UNION ALL SELECT * FROM input_1"}));

        // ─── Rule Engine ───
        def!("org.knime.base.node.preproc.rules.RuleEngineNodeFactory", "core.transform.condition",
            |s| json!({"rules": s.get("rules").cloned().unwrap_or(json!([]))}));

        // ─── Python ───
        def!("org.knime.base.node.preproc.python.PythonScriptNodeFactory", "core.script.python",
            |s| json!({"script": s.get("python_script").and_then(|v| v.as_str()).unwrap_or("")}));

        // ─── SQL/Database ───
        def!("org.knime.base.node.io.database.DBReaderNodeFactory", "core.sql",
            |s| json!({"query": s.get("sql_string").and_then(|v| v.as_str()).unwrap_or(""),
                       "connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or("")}));

        // ─── IO (v3 / alternative factories) ───
        def!("org.knime.base.node.io.tablecreator.TableCreator2NodeFactory", "core.io.csv_read",
            |_| json!({}));
        def!("org.knime.ext.poi3.node.io.filehandling.excel.reader.ExcelTableReaderNodeFactory", "core.io.excel_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "sheet_name": s.get("sheet_name").and_then(|v| v.as_str())}));
        def!("org.knime.ext.poi3.node.io.filehandling.excel.writer.ExcelTableWriterNodeFactory", "core.io.excel_write",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.base.node.io.filehandling.csv.reader.CSVTableReaderNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "has_header": s.get("has_column_header").and_then(|v| v.as_bool()).unwrap_or(true)}));
        def!("org.knime.base.node.io.filehandling.table.reader.KnimeTableReaderNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("")}));

        // ─── Transform (v2/v3 / alternative factories) ───
        def!("org.knime.base.node.preproc.rowkey2.RowKeyNodeFactory2", "core.transform.project",
            |_| json!({"add_rowid": true}));
        def!("org.knime.base.node.preproc.constantvalue.ConstantValueColumnNodeFactory", "core.transform.project",
            |s| json!({"constant_column": s.get("column_name").and_then(|v| v.as_str()),
                       "constant_value": s.get("value").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.preproc.stringmanipulation.StringManipulationNodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "expression": s.get("expression").and_then(|v| v.as_str())}));
        def!("org.knime.expressions.base.node.formulas.FormulasNodeFactory", "core.transform.project",
            |s| json!({"expressions": s.get("formulas")}));
        def!("org.knime.ext.jep.JEPNodeFactory", "core.transform.project",
            |s| json!({"expression": s.get("expression").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.rules.engine.RuleEngineNodeFactory", "core.transform.condition",
            |s| json!({"rules": s.get("rules").cloned().unwrap_or(json!([]))}));
        def!("org.knime.base.node.rules.engine.RuleEngineFilterNodeFactory", "core.transform.filter",
            |s| json!({"rules": s.get("rules").cloned().unwrap_or(json!([]))}));
        def!("org.knime.base.node.preproc.pivot.Pivot2NodeFactory", "Pivot",
            |_| json!({}));
        def!("org.knime.base.node.preproc.joiner3.Joiner3NodeFactory", "core.sql",
            |s| json!({"query": Self::build_join_sql(s)}));
        def!("org.knime.base.node.preproc.filter.column.DataColumnSpecFilterNodeFactory", "core.transform.project",
            |s| json!({"columns": s.get("included_column_names").and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())}));
        def!("org.knime.base.node.preproc.table.cropper.TableCropperNodeFactory", "core.transform.filter",
            |s| json!({"row_range": s.get("row_range")}));
        def!("org.knime.base.node.preproc.transpose.TransposeTableNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "transpose not yet supported"}));
        def!("org.knime.base.node.util.sampledata.SampleDataNodeFactory", "core.transform.filter",
            |s| json!({"sample_size": s.get("sample_size"), "seed": s.get("seed")}));

        // ─── Loop / Flow ───
        def!("org.knime.base.node.meta.looper.LoopEndDynamicNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "loop end"}));
        def!("org.knime.base.node.flowvariable.tablerowtovariable3.TableToVariable3NodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.util.mergevariables.MergeVariables2NodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.meta.looper.variable.start.LoopStartVariable3NodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "loop start"}));

        // ─── Viz / View / Dialog (passthrough data — Shaper handles visualization)
        def!("org.knime.base.views.node.textview.TextViewNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.configuration.input.integer.IntegerDialogNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.js.base.node.configuration.selection.value.ValueSelectionDialogNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.dynamic.js.v30.DynamicJSNodeFactory", "core.script.python",
            |s| json!({"script": s.get("js_script").and_then(|v| v.as_str()).unwrap_or(""),
                       "type": "javascript"}));
        def!("org.knime.base.node.viz.property.color.ColorManager2NodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Virtual / Infra (pass-through) ───
        def!("org.knime.core.node.workflow.virtual.subnode.VirtualSubNodeInputNodeFactory", "core.transform.project",
            |_| json!({}));
        def!("org.knime.core.node.workflow.virtual.subnode.VirtualSubNodeOutputNodeFactory", "core.transform.project",
            |_| json!({}));

        // ─── Flow Variables ───
        def!("org.knime.ext.jep.variable.JEPVariableNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.rules.engine.RuleEngineVariableNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.util.mergevariables.MergeVariablesNodeFactory", "core.flow.variable_creator",
            |_| json!({}));

        // ─── Collection / Loop ───
        def!("org.knime.base.collection.list.create2.CollectionCreate2NodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "collection create"}));

        // ─── Sort (alt namespace) ───
        def!("org.knime.base.node.preproc.sorter.SorterNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let asc = !s.get("sort_descending").and_then(|v| v.as_bool()).unwrap_or(false);
                let order = if asc { "ASC" } else { "DESC" };
                json!({"query": format!("SELECT * FROM input_0 ORDER BY {} {}", col, order)})
            });

        // ─── Column Ops ───
        def!("org.knime.base.node.preproc.column.renamer.ColumnRenamerNodeFactory", "core.transform.project",
            |s| json!({"rename": s.get("column_name").and_then(|v| v.as_str()),
                       "new_name": s.get("new_column_name").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.preproc.columnresorter.ColumnResorterNodeFactory", "core.transform.project",
            |s| json!({"columns": s.get("column_names").and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())}));
        def!("org.knime.base.node.preproc.columnrenameregex.ColumnRenameRegexNodeFactory", "core.transform.project",
            |s| json!({"pattern": s.get("pattern"), "replacement": s.get("replacement")}));

        // ─── Row Ops ───
        def!("org.knime.base.node.preproc.append.row.AppendedRowsNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0 UNION ALL SELECT * FROM input_1"}));
        def!("org.knime.base.node.preproc.duplicates.DuplicateRowFilterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "dedup"}));
        def!("org.knime.base.node.preproc.filter.row.RowFilter2PortNodeFactory", "core.transform.filter",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let op = s.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
                let val = s.get("filter_value").and_then(|v| v.as_str()).unwrap_or("");
                json!({"expr": format!("{} {} {}", col, op, val), "split": true})
            });

        // ─── Join / Pivot ───
        def!("org.knime.base.node.preproc.crossjoin.CrossJoinerNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0 CROSS JOIN input_1"}));
        def!("org.knime.base.node.preproc.joiner.Joiner2NodeFactory", "core.sql",
            |s| json!({"query": Self::build_join_sql(s)}));
        def!("org.knime.base.node.preproc.unpivot2.Unpivot2NodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "unpivot"}));

        // ─── Cell Ops ───
        def!("org.knime.base.node.preproc.cellsplit2.CellSplitter2NodeFactory", "core.sql",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "delimiter": s.get("delimiter").and_then(|v| v.as_str())}));

        // ─── IO ───
        def!("org.knime.base.node.io.table.read.ReadTableNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("")}));

        // ─── Util ───
        def!("org.knime.base.node.util.extracttabledimension.ExtractTableDimensionNodeFactory", "core.transform.profiler",
            |_| json!({}));

        // ─── Viz / View ───
        def!("org.knime.base.views.node.tableview.TableViewNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.barchart.BarChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.bar.BarChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── QuickForm / JS Widgets ───
        def!("org.knime.js.base.node.quickform.input.integer.IntInputQuickFormNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.js.base.node.quickform.input.string.StringInputQuickFormNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.js.base.node.css.editor.CSSEditorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "css editor"}));

        // ─── Time ───
        def!("org.knime.time.node.create.createdatetime.CreateDateTimeNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "datetime create"}));

        Self { type_map }
    }

    /// Resolve a KNIME node (by factory class name) to its SwanFlow type + config.
    /// Looks for the factory in both node_type and settings JSON.
    pub fn resolve_node(
        &self,
        node_type: Option<&str>,
        settings: &serde_json::Value,
    ) -> (String, serde_json::Value) {
        let factory = extract_factory(node_type, settings);
        if let Some(def) = self.type_map.get(&factory) {
            (def.swanflow_type.clone(), (def.config_fn)(settings))
        } else {
            ("core.util.unknown".to_string(), serde_json::json!({}))
        }
    }

    pub async fn convert(&self, archive: KnwfArchive) -> KnimeResult<Workflow> {
        let mut graph = petgraph::stable_graph::StableGraph::<NodeInstance, EdgeInstance>::new();
        let mut id_to_idx: HashMap<String, NodeIndex> = HashMap::new();

        // Add nodes to petgraph
        for knime_node in &archive.nodes {
            let factory = extract_factory(knime_node.node_type.as_deref(), &knime_node.settings);
            let mapped = self.type_map.get(&factory);

            let (swanflow_type, config) = if let Some(def) = mapped {
                (def.swanflow_type.clone(), (def.config_fn)(&knime_node.settings))
            } else {
                tracing::warn!(
                    "Unknown KNIME node type: {} (id={}), treating as pass-through",
                    factory,
                    knime_node.id
                );
                ("pass_through".to_string(), json!({}))
            };

            let instance = NodeInstance {
                id: Uuid::new_v4(),
                type_id: swanflow_type,
                config,
                state: NodeState::Configured,
                materialize: false,
                sub_workflow: None,
                mode: StepMode::Sequential,
                error_mode: swanflow_core::graph::ErrorMode::Fail,
                max_retries: 0,
                until: None,
                max_iterations: 10,
                condition: None,
                when: None,
                output_var: knime_node.name.clone(),
            };

            let idx = graph.add_node(instance);
            id_to_idx.insert(knime_node.id.clone(), idx);
        }

        // Add edges
        for edge in &archive.edges {
            if let (Some(&src), Some(&tgt)) = (
                id_to_idx.get(&edge.source_id),
                id_to_idx.get(&edge.target_id),
            ) {
                graph.add_edge(
                    src,
                    tgt,
                    EdgeInstance {
                        source_port: edge.source_port,
                        target_port: edge.target_port,
                    },
                );
            }
        }

        // Set materialize=true on terminal nodes (no outgoing edges) for --print support
        {
            let has_outgoing: HashSet<NodeIndex> = graph
                .edge_indices()
                .filter_map(|e| graph.edge_endpoints(e))
                .map(|(src, _)| src)
                .collect();
            let terminal_nodes: Vec<NodeIndex> = graph
                .node_indices()
                .filter(|idx| !has_outgoing.contains(idx))
                .collect();
            for idx in terminal_nodes {
                if let Some(node) = graph.node_weight_mut(idx) {
                    node.materialize = true;
                }
            }
        }

        Ok(Workflow {
            id: Uuid::new_v4(),
            name: "Imported KNIME Workflow".to_string(),
            context: HashMap::new(),
            graph,
        })
    }

    /// Build a SQL JOIN query from KNIME joiner settings.
    fn build_join_sql(s: &serde_json::Value) -> String {
        let left_keys: Vec<&str> = s
            .get("left_column_names")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let right_keys: Vec<&str> = s
            .get("right_column_names")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let join_type = s
            .get("join_type")
            .and_then(|v| v.as_str())
            .unwrap_or("INNER");

        let conditions: Vec<String> = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(l, r)| format!("input_0.{} = input_1.{}", l, r))
            .collect();

        if conditions.is_empty() {
            format!("SELECT * FROM input_0 {} JOIN input_1", join_type)
        } else {
            format!(
                "SELECT * FROM input_0 {} JOIN input_1 ON {}",
                join_type,
                conditions.join(" AND ")
            )
        }
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
            _ => knime_op,
        }
    }

    /// List all registered mappings for reporting/coverage analysis.
    pub fn list_all(&self) -> Vec<MappingEntry> {
        self.type_map
            .iter()
            .map(|(factory, def)| MappingEntry {
                factory: factory.clone(),
                sw_type: def.swanflow_type.clone(),
                category: Self::categorize(factory).to_string(),
                compat: Self::compat_level(&def.swanflow_type).to_string(),
            })
            .collect()
    }

    fn categorize(factory: &str) -> &'static str {
        if factory.contains(".io.") || factory.contains(".filehandling.") {
            "IO"
        } else if factory.contains(".preproc.") || factory.contains("transform") {
            "Transform"
        } else if factory.contains("statistics")
            || factory.contains("mining")
            || factory.contains("cluster")
        {
            "Analysis"
        } else if factory.contains("views")
            || factory.contains("viz")
            || factory.contains("plotter")
            || factory.contains("chart")
        {
            "Visualization"
        } else if factory.contains("loop")
            || factory.contains("flowvariable")
            || factory.contains("variable")
            || factory.contains("meta")
            || factory.contains("collection")
        {
            "FlowControl"
        } else if factory.contains("python")
            || factory.contains("script")
            || factory.contains("jep")
        {
            "Script"
        } else if factory.contains("quickform")
            || factory.contains("dialog")
            || factory.contains("css")
        {
            "UIWidget"
        } else {
            "Other"
        }
    }

    fn compat_level(sw_type: &str) -> &'static str {
        match sw_type {
            "core.missing.placeholder" => "Unsupported",
            "pass_through" => "Passthrough",
            t if t.starts_with("core.transform.profiler") => "Degraded",
            t if t == "core.sql" => "Emulated",
            _ => "Full",
        }
    }
}

impl Default for KnimeNodeMapper {
    fn default() -> Self {
        Self::new()
    }
}

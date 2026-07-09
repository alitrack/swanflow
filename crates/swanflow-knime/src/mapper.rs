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
    /// Fallback approach when the primary extension is unavailable (e.g. DuckDB extension not loaded)
    pub fallback: Option<String>,
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
                        fallback: None,
                    },
                );
            };
        }

        /// Like def! but with a fallback description for when the primary extension is unavailable
        macro_rules! def_fb {
            ($knime:expr, $sw_type:expr, $cfg:expr, $fallback:expr) => {
                type_map.insert(
                    $knime.into(),
                    SwanFlowNodeDef {
                        swanflow_type: $sw_type.into(),
                        config_fn: Box::new($cfg),
                        fallback: Some($fallback.into()),
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
        def!("org.knime.base.node.preproc.transpose.TransposeTableNodeFactory", "core.sql",
            |s| {
                let pivot_col = s.get("pivot_column").and_then(|v| v.as_str()).unwrap_or("");
                let value_col = s.get("value_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("PIVOT input_0 ON {} USING FIRST({})", pivot_col, value_col)})
            });
        def!("org.knime.base.node.util.sampledata.SampleDataNodeFactory", "core.transform.filter",
            |s| json!({"sample_size": s.get("sample_size"), "seed": s.get("seed")}));

        // ─── Loop / Flow ───
        // KNIME loop nodes are metanodes that contain the loop body as sub-workflow.
        // They map to SwanFlow's StepMode::Loop with sub_workflow resolved from archive.metanodes.
        def!("org.knime.base.node.meta.looper.LoopEndDynamicNodeFactory", "core.flow.loop",
            |_| json!({"loop_kind": "generic"}));
        def!("org.knime.base.node.flowvariable.tablerowtovariable3.TableToVariable3NodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.util.mergevariables.MergeVariables2NodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.meta.looper.variable.start.LoopStartVariable3NodeFactory", "core.flow.loop",
            |_| json!({"loop_kind": "variable"}));
        def!("org.knime.base.node.meta.looper.LoopStartNodeFactory", "core.flow.loop",
            |_| json!({"loop_kind": "generic"}));
        def!("org.knime.base.node.meta.looper.LoopEndNodeFactory", "core.flow.loop",
            |_| json!({"loop_kind": "generic"}));
        def!("org.knime.base.node.meta.looper.ChunkLoopStartNodeFactory", "core.flow.loop",
            |s| json!({"loop_kind": "chunk", "chunk_size": s.get("chunk_size").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(100)}));
        def!("org.knime.base.node.meta.looper.CountingLoopStartNodeFactory", "core.flow.loop",
            |s| json!({"loop_kind": "counting", "iterations": s.get("number_of_iterations").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(10)}));
        def!("org.knime.base.node.meta.looper.RecursiveLoopStartNodeFactory", "core.flow.loop",
            |s| json!({"loop_kind": "recursive", "max_depth": s.get("max_recursion_depth").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(10)}));
        def!("org.knime.base.node.meta.looper.RecursiveLoopEndNodeFactory", "core.flow.loop",
            |_| json!({"loop_kind": "recursive"}));

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
        def!("org.knime.base.collection.list.create2.CollectionCreate2NodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT list(\"{}\") AS collection FROM input_0", col)})
            });

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
        def!("org.knime.base.node.preproc.duplicates.DuplicateRowFilterNodeFactory", "core.sql",
            |s| {
                let cols = s.get("column_names").and_then(|v| v.as_str()).unwrap_or("*");
                json!({"query": format!("SELECT DISTINCT ON ({}) * FROM input_0", cols)})
            });
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
        def!("org.knime.base.node.preproc.unpivot2.Unpivot2NodeFactory", "core.sql",
            |s| {
                let cols_str = s.get("included_column_names").and_then(|v| v.as_str()).unwrap_or("");
                let cols: Vec<&str> = cols_str.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).collect();
                let cols_list = cols.iter().map(|c| format!("'{}'", c)).collect::<Vec<_>>().join(", ");
                json!({"query": format!("UNPIVOT input_0 ON COLUMNS({}) INTO NAME key VALUE val", cols_list)})
            });

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
        def!("org.knime.js.base.node.css.editor.CSSEditorNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Time ───
        def!("org.knime.time.node.create.createdatetime.CreateDateTimeNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("ts");
                let fmt = s.get("format").and_then(|v| v.as_str()).unwrap_or("%Y-%m-%d %H:%M:%S");
                json!({"query": format!("SELECT *, strptime(CAST({} AS VARCHAR), '{}') AS {}_dt FROM input_0", col, fmt, col)})
            });
        def!("org.knime.time.node.extract.datetime.ExtractDateTimeFieldNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let fields = s.get("extracted_fields").and_then(|v| v.as_str()).unwrap_or("year");
                json!({"query": format!("SELECT *, EXTRACT({} FROM {}) AS extracted_{} FROM input_0", fields, col, fields)})
            });
        def!("org.knime.time.node.convert.stringtoduration.StringToDurationNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, datediff('second', '1970-01-01'::TIMESTAMP, {}::TIMESTAMP) AS {}_duration FROM input_0", col, col)})
            });
        def!("org.knime.time.node.shift.time.ShiftTimeNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let amount = s.get("shift_amount").and_then(|v| v.as_str()).unwrap_or("1");
                let unit = s.get("shift_unit").and_then(|v| v.as_str()).unwrap_or("day");
                json!({"query": format!("SELECT *, {} + INTERVAL '{}' {} AS {}_shifted FROM input_0", col, amount, unit, col)})
            });
        def!("org.knime.time.node.create.datetimerange.DateTimeRangeNodeFactory", "core.sql",
            |s| {
                let start = s.get("start_column").and_then(|v| v.as_str()).unwrap_or("'2020-01-01'::DATE");
                let end = s.get("end_column").and_then(|v| v.as_str()).unwrap_or("'2020-12-31'::DATE");
                let step = s.get("step").and_then(|v| v.as_str()).unwrap_or("INTERVAL 1 DAY");
                json!({"query": format!("SELECT UNNEST(generate_series({}::TIMESTAMP, {}::TIMESTAMP, {})) AS dt", start, end, step)})
            });
        def!("org.knime.time.node.extract.timefield.ExtractTimeFieldNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, EXTRACT(HOUR FROM {}) AS hour, EXTRACT(MINUTE FROM {}) AS minute FROM input_0", col, col)})
            });

        // ─── ETL / DB Write ───
        def!("org.knime.base.node.io.database.DBWriterNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.base.node.io.database.DBInsertNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "mode": "insert"}));
        def!("org.knime.base.node.io.database.DBDeleteNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "mode": "delete"}));
        def!("org.knime.base.node.io.database.DBUpdateNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "mode": "update"}));
        def_fb!("org.knime.base.node.io.filestore.FileDownloadNodeFactory", "core.sql",
            |s| {
                let url = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                // Detect cloud provider and use appropriate DuckDB extension protocol
                let (proto, path) = if url.starts_with("spfs://") || url.contains("sharepoint") {
                    ("spfs", url.trim_start_matches("spfs://"))
                } else if url.starts_with("odfs://") || url.contains("onedrive") {
                    ("odfs", url.trim_start_matches("odfs://"))
                } else if url.starts_with("gdfs://") || url.contains("drive.google") {
                    ("gdfs", url.trim_start_matches("gdfs://"))
                } else if url.starts_with("dbxfs://") || url.contains("dropbox") {
                    ("dbxfs", url.trim_start_matches("dbxfs://"))
                } else if url.starts_with("sftp://") {
                    ("sftp", url.trim_start_matches("sftp://"))
                } else if url.starts_with("s3://") {
                    ("s3", url.trim_start_matches("s3://"))
                } else if url.starts_with("http://") || url.starts_with("https://") {
                    ("httpfs", url)
                } else {
                    ("file", url)
                };
                let ext = if path.ends_with(".parquet") { "parquet" }
                    else if path.ends_with(".csv") { "csv" }
                    else if path.ends_with(".json") { "json" }
                    else { "auto" };
                json!({"query": format!("SELECT * FROM read_{}('{}://{}')", ext, proto, path)})
            }, "Fallback: SwanFlow Python UDF → requests.get()");
        def!("org.knime.base.node.io.filehandling.node.FileListDirectoryNodeFactory", "core.sql",
            |s| {
                let dir = s.get("directory").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM glob('{}**')", dir)})
            });

        // ─── REST / HTTP (erpl-web / http_client powered) ───
        def!("org.knime.rest.nodes.GETRequestNodeFactory", "core.sql",
            |s| {
                let url = s.get("url").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM read_json_auto('{}')", url)})
            });
        def!("org.knime.rest.nodes.POSTRequestNodeFactory", "core.sql",
            |s| {
                let url = s.get("url").and_then(|v| v.as_str()).unwrap_or("");
                let body = s.get("body").and_then(|v| v.as_str()).unwrap_or("{}");
                json!({"query": format!(
                    "SELECT http_post('{}', headers := '{{}}', body := '{}') AS response", url, body)})
            });
        def!("org.knime.rest.nodes.PUTRequestNodeFactory", "core.sql",
            |s| {
                let url = s.get("url").and_then(|v| v.as_str()).unwrap_or("");
                let body = s.get("body").and_then(|v| v.as_str()).unwrap_or("{}");
                json!({"query": format!(
                    "SELECT http_put('{}', headers := '{{}}', body := '{}') AS response", url, body)})
            });

        // ─── Transform — String / Regex ───
        def!("org.knime.base.node.preproc.stringreplacer.StringReplaceNodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "pattern": s.get("pattern").and_then(|v| v.as_str()),
                       "replacement": s.get("replacement").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.preproc.regexsplit.RegexSplitNodeFactory", "core.sql",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "pattern": s.get("pattern").and_then(|v| v.as_str())}));

        // ─── Transform — Normalize / Scale ───
        def!("org.knime.base.node.preproc.normalize.NormalizerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let method = s.get("normalization_method").and_then(|v| v.as_str()).unwrap_or("minmax");
                let q = if method == "zscore" {
                    format!("SELECT *, ({} - AVG({}) OVER()) / STDDEV({}) OVER() AS {}_norm FROM input_0", col, col, col, col)
                } else {
                    format!("SELECT *, ({} - MIN({}) OVER()) / NULLIF(MAX({}) OVER() - MIN({}) OVER(), 0) AS {}_norm FROM input_0", col, col, col, col, col)
                };
                json!({"query": q})
            });
        def!("org.knime.base.node.preproc.normalize.NormalizerNodeFactory2", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, ({} - MIN({}) OVER()) / NULLIF(MAX({}) OVER() - MIN({}) OVER(), 0) AS {}_norm FROM input_0", col, col, col, col, col)})
            });
        def!("org.knime.base.node.preproc.normalize.NormalizerNodeFactory3", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, ({} - AVG({}) OVER()) / NULLIF(STDDEV({}) OVER(), 0) AS {}_zscore FROM input_0", col, col, col, col)})
            });

        // ─── Transform — Round / Number ───
        def!("org.knime.base.node.preproc.rounddouble.RoundDoubleNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let digits = s.get("decimal_places").and_then(|v| v.as_str()).unwrap_or("0");
                json!({"query": format!("SELECT *, ROUND({}, {}) AS {}_rounded FROM input_0", col, digits, col)})
            });
        def!("org.knime.base.node.util.numberrounder.NumberRounderNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, ROUND({}) FROM input_0", col)})
            });

        // ─── Transform — Binning / Rank ───
        def!("org.knime.base.node.preproc.binner.BinnerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let bins = s.get("bins").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CASE {} END AS {}_bin FROM input_0", bins, col)})
            });
        def!("org.knime.base.node.preproc.autobinner.AutoBinnerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let n = s.get("num_bins").and_then(|v| v.as_str()).unwrap_or("10");
                json!({"query": format!("SELECT *, WIDTH_BUCKET({}, MIN({}) OVER(), MAX({}) OVER() + 1, {}) AS {}_bin FROM input_0", col, col, col, n, col)})
            });
        def!("org.knime.base.node.preproc.rank.RankNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let asc = !s.get("sort_descending").and_then(|v| v.as_bool()).unwrap_or(false);
                let order = if asc { "ASC" } else { "DESC" };
                json!({"query": format!("SELECT *, ROW_NUMBER() OVER (ORDER BY {} {}) AS rank FROM input_0", col, order)})
            });

        // ─── Transform — Lag / Moving Average ───
        def!("org.knime.base.node.preproc.lag.LagColumnNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let n = s.get("lag").and_then(|v| v.as_str()).unwrap_or("1");
                json!({"query": format!("SELECT *, LAG({}, {}) OVER () AS {}_lag FROM input_0", col, n, col)})
            });
        def!("org.knime.base.node.preproc.movingaverage.MovingAverageNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let window = s.get("window_size").and_then(|v| v.as_str()).unwrap_or("3");
                json!({"query": format!("SELECT *, AVG({}) OVER (ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS {}_ma FROM input_0", col, window, col)})
            });

        // ─── Transform — One-to-Many / Many-to-One ───
        def!("org.knime.base.node.preproc.one2many.OneToManyNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let delim = s.get("delimiter").and_then(|v| v.as_str()).unwrap_or(",");
                json!({"query": format!("SELECT *, UNNEST(string_split(\"{}\", '{}')) AS {}_expanded FROM input_0", col, delim, col)})
            });
        def!("org.knime.base.node.preproc.many2one.ManyToOneNodeFactory", "core.sql",
            |s| {
                let group_col = s.get("group_column").and_then(|v| v.as_str()).unwrap_or("");
                let agg_col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let sep = s.get("separator").and_then(|v| v.as_str()).unwrap_or(",");
                json!({"query": format!("SELECT {}, string_agg(\"{}\", '{}') AS {}_combined FROM input_0 GROUP BY {}", group_col, agg_col, sep, agg_col, group_col)})
            });

        // ─── Transform — Column Type ───
        def!("org.knime.base.node.preproc.colconvert.ColumnConverterNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let target = s.get("target_type").and_then(|v| v.as_str()).unwrap_or("VARCHAR");
                json!({"query": format!("SELECT *, CAST({} AS {}) AS {}_cast FROM input_0", col, target, col)})
            });
        def!("org.knime.base.node.preproc.domain.DomainCalculatorNodeFactory", "core.transform.profiler",
            |_| json!({}));
        def!("org.knime.base.node.preproc.colconvert.stringtonumber.StringToNumberNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CAST({} AS DOUBLE) AS {}_num FROM input_0", col, col)})
            });
        def!("org.knime.base.node.preproc.colconvert.numbertostring.NumberToStringNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CAST({} AS VARCHAR) AS {}_str FROM input_0", col, col)})
            });

        // ─── Transform — Missing Value variants ───
        def!("org.knime.base.node.preproc.missingval.handler.MissingValueColumnFilterNodeFactory", "core.quality.null_check",
            |s| json!({"strategy": "drop_col", "threshold": s.get("threshold").and_then(|v| v.as_f64()).unwrap_or(0.5)}));
        def!("org.knime.base.node.preproc.filter.row.ReferenceRowFilterNodeFactory", "core.transform.filter",
            |s| json!({"expr": s.get("filter_criteria").and_then(|v| v.as_str()).unwrap_or("1=1")}));
        def!("org.knime.base.node.preproc.row.filter.NominalValueRowFilterNodeFactory", "core.transform.filter",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let vals = s.get("included_values").and_then(|v| v.as_str()).unwrap_or("");
                json!({"expr": format!("{} IN ({})", col, vals)})
            });

        // ─── Flow Control — Switch / IF / Try-Catch ───
        def!("org.knime.base.node.meta.switchend.SwitchEndNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "switch end"}));
        def!("org.knime.base.node.meta.extractvariable.ExtractVariableNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.meta.trycatch.TryCatchNodeFactory", "core.flow.try_catch",
            |_| json!({}));
        def!("org.knime.base.node.meta.switch3.Switch3NodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "switch case"}));

        // ─── Loop Start variants ───
        def!("org.knime.base.node.meta.looper.counting.loopstart.LoopStartCountingNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "counting loop start"}));
        def!("org.knime.base.node.meta.looper.interval.LoopStartIntervalNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "interval loop start"}));
        def!("org.knime.base.node.meta.looper.generic.LoopStartGenericNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "generic loop start"}));
        def!("org.knime.base.node.meta.looper.table.LoopStartTableNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "table row loop start"}));
        def!("org.knime.base.node.meta.looper.file.LoopStartFileNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file list loop start"}));

        // ─── Loop End variants ───
        def!("org.knime.base.node.meta.looper.table.LoopEndTableNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "table loop end"}));
        def!("org.knime.base.node.meta.looper.file.LoopEndFileNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file loop end"}));

        // ─── Flow Variable nodes ───
        def!("org.knime.base.node.flowvariable.tablerowtovariable2.TableRowToVariable2NodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.flowvariable.tablerowtovariable.TableRowToVariableNodeFactory", "core.flow.variable_creator",
            |_| json!({}));
        def!("org.knime.base.node.flowvariable.variabletoreaderow.VariableToTableRowNodeFactory", "core.flow.variable_creator",
            |_| json!({}));

        // ─── Timer / Wait ───
        def!("org.knime.base.node.meta.timedloop.TimedLoopStartNodeFactory", "core.sql",
            |s| {
                let seconds = s.get("interval_seconds").and_then(|v| v.as_str()).unwrap_or("60");
                json!({"query": format!("SELECT pg_sleep({}) AS waited, * FROM input_0", seconds)})
            });

        // ─── Workflow / SSH ───
        def!("org.knime.base.node.meta.caller.WorkflowExecutorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "sub-workflow executor (use SwanFlow sub_workflow)"}));
        def!("org.knime.ext.ssh.SSHConnectionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "ssh connection"}));
        def!("org.knime.ext.ssh.SSHCommandNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SSH command → use ducklink WASM component or SwanFlow Python UDF with paramiko"}));

        // ─── Analysis — Regression (duckdb-ml powered) ───
        // ml_train() takes: model_name, algorithm, target_json, features_json, params_json
        // KNIME settings: target_column, feature_columns (comma-separated)
        def_fb!("org.knime.base.node.mine.regression.linear.LinearRegressionNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_linreg', 'linear_regression', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            }, "Fallback: SwanFlow Python UDF → sklearn.linear_model.LinearRegression");
        def_fb!("org.knime.base.node.mine.regression.logistic.LogisticRegressionNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_logreg', 'logistic_regression', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            }, "Fallback: SwanFlow Python UDF → sklearn.linear_model.LogisticRegression");
        def!("org.knime.base.node.mine.regression.polynomial.PolynomialRegressionNodeFactory", "core.sql",
            |s| {
                // duckdb-ml doesn't have polynomial regression; use linear with feature expansion hint
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let degree = s.get("polynomial_degree").and_then(|v| v.as_str()).unwrap_or("2");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_polyreg', 'linear_regression', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"poly_degree\": {}}}')",
                    target, feat_json_array, degree)})
            });

        // ─── Analysis — Tree / Ensemble (duckdb-ml powered) ───
        def_fb!("org.knime.base.node.mine.decisiontree2.DecisionTreeLearnerNodeFactory2", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_dt', 'decision_tree', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            }, "Fallback: SwanFlow Python UDF → sklearn.tree.DecisionTreeRegressor");
        def!("org.knime.base.node.mine.decisiontree2.DecisionTreePredictorNodeFactory2", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_args: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                json!({"query": format!(
                    "SELECT *, (SELECT prediction FROM ml_predict('sw_dt', {})) AS prediction FROM input_0",
                    feat_args.join(", "))})
            });
        def_fb!("org.knime.base.node.mine.treeensemble.randomforest.RandomForestLearnerNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                let n_trees = s.get("number_of_trees").and_then(|v| v.as_str()).unwrap_or("100");
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_rf', 'random_forest', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"n_estimators\": {}}}')",
                    target, feat_json_array, n_trees)})
            }, "Fallback: SwanFlow Python UDF → sklearn.ensemble.RandomForestRegressor");
        def!("org.knime.base.node.mine.treeensemble.randomforest.RandomForestPredictorNodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_args: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                json!({"query": format!(
                    "SELECT *, (SELECT prediction FROM ml_predict('sw_rf', {})) AS prediction FROM input_0",
                    feat_args.join(", "))})
            });
        def_fb!("org.knime.base.node.mine.treeensemble.gradientboost.GradientBoostLearnerNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                let n_trees = s.get("number_of_trees").and_then(|v| v.as_str()).unwrap_or("100");
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_gbdt', 'xgboost_regression', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"n_estimators\": {}}}')",
                    target, feat_json_array, n_trees)})
            }, "Fallback: SwanFlow Python UDF → xgboost.XGBRegressor");

        // ─── Analysis — Clustering (duckdb-ml powered) ───
        def_fb!("org.knime.base.node.mine.cluster.kmeans.KMeansNodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let n_clusters = s.get("number_of_clusters").and_then(|v| v.as_str()).unwrap_or("3");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_kmeans', 'kmeans', (SELECT json_group_array(0) FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"k\": {}}}')",
                    feat_json_array, n_clusters)})
            }, "Fallback: SwanFlow Python UDF → sklearn.cluster.KMeans");
        def!("org.knime.base.node.mine.cluster.hierarchical.HierarchicalClusterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "hierarchical clustering (not in duckdb-ml; use KMeans or SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.cluster.dbscan.DBSCANNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "DBSCAN clustering (not in duckdb-ml yet)"}));

        // ─── Analysis — Dimension Reduction (duckdb-ml powered) ───
        def!("org.knime.base.node.mine.pca.PCANodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let n_components = s.get("number_of_components").and_then(|v| v.as_str()).unwrap_or("2");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_pca', 'pca', (SELECT json_group_array(0) FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"n_components\": {}}}')",
                    feat_json_array, n_components)})
            });

        // ─── Analysis — Statistics (DuckDB built-in + stats_duck) ───
        def!("org.knime.base.node.stats.correlation.CorrelationNodeFactory", "core.sql",
            |s| {
                let cols_str = s.get("included_column_names").and_then(|v| v.as_str()).unwrap_or("");
                let cols: Vec<&str> = cols_str.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).collect();
                if cols.len() < 2 {
                    json!({"query": "SELECT 'Need at least 2 columns' AS error FROM input_0"})
                } else {
                    let cols_ref: Vec<String> = cols.iter().map(|c| c.to_string()).collect();
                    let pairs: Vec<String> = (0..cols_ref.len()).flat_map(|i| {
                        let c = cols_ref.clone();
                        ((i+1)..cols_ref.len()).map(move |j| {
                            format!("corr(\"{}\", \"{}\") AS {}_{}_corr", c[i], c[j], c[i], c[j])
                        })
                    }).collect();
                    json!({"query": format!("SELECT {} FROM input_0", pairs.join(", "))})
                }
            });
        def_fb!("org.knime.base.node.stats.shapiro.ShapiroWilkNodeFactory", "core.sql",
            |s| {
                let col = s.get("included_column_names").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_shapiro_wilk(\"{}\") AS shapiro_wilk FROM input_0", col)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.shapiro");
        def_fb!("org.knime.base.node.stats.ttest.TTestNodeFactory", "core.sql",
            |s| {
                let col1 = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let col2 = s.get("second_column").and_then(|v| v.as_str()).unwrap_or("");
                let test_type = s.get("test_type").and_then(|v| v.as_str()).unwrap_or("independent");
                let query = if test_type == "paired" {
                    format!("SELECT stats_duck_ttest_paired(\"{}\", \"{}\") AS t_test FROM input_0", col1, col2)
                } else {
                    format!("SELECT stats_duck_ttest_independent(\"{}\", \"{}\") AS t_test FROM input_0", col1, col2)
                };
                json!({"query": query})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.ttest_ind / ttest_rel");
        def_fb!("org.knime.base.node.stats.anova.ANOVANodeFactory", "core.sql",
            |s| {
                let group_col = s.get("group_column").and_then(|v| v.as_str()).unwrap_or("");
                let value_col = s.get("value_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_anova(\"{}\", \"{}\") AS anova FROM input_0", group_col, value_col)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.f_oneway");
        def_fb!("org.knime.base.node.stats.chisquare.ChiSquareNodeFactory", "core.sql",
            |s| {
                let col1 = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let col2 = s.get("second_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_chisq(\"{}\", \"{}\") AS chi_square FROM input_0", col1, col2)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.chi2_contingency");

        // ─── Analysis — Naive Bayes / SVM / MLP (duckdb-ml powered) ───
        def_fb!("org.knime.base.node.mine.bayes.naivebayes.NaiveBayesLearnerNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_nb', 'naive_bayes', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            }, "Fallback: SwanFlow Python UDF → sklearn.naive_bayes.GaussianNB");
        def!("org.knime.base.node.mine.bayes.naivebayes.NaiveBayesPredictorNodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_args: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                json!({"query": format!(
                    "SELECT *, (SELECT prediction FROM ml_predict('sw_nb', {})) AS prediction FROM input_0",
                    feat_args.join(", "))})
            });
        def!("org.knime.base.node.mine.svm.SVMLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SVM learner (not in duckdb-ml; use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.svm.SVMPredictorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SVM predictor (not in duckdb-ml; use SwanFlow Python UDF)"}));
        def_fb!("org.knime.base.node.mine.mlp.MLPLearnerNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                let hidden = s.get("hidden_layers").and_then(|v| v.as_str()).unwrap_or("64");
                let lr = s.get("learning_rate").and_then(|v| v.as_str()).unwrap_or("0.01");
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_mlp', 'mlp_regressor', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"hidden_size\": {}, \"lr\": {}}}')",
                    target, feat_json_array, hidden, lr)})
            }, "Fallback: SwanFlow Python UDF → sklearn.neural_network.MLPRegressor");
        def!("org.knime.base.node.mine.mlp.MLPPredictorNodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_args: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                json!({"query": format!(
                    "SELECT *, (SELECT prediction FROM ml_predict('sw_mlp', {})) AS prediction FROM input_0",
                    feat_args.join(", "))})
            });

        // ─── Analysis — Scoring / PMML (duckdb-ml AutoML + model export) ───
        def!("org.knime.base.node.mine.scorer.accuracy.AccuracyScorerNodeFactory", "core.sql",
            |s| {
                let target = s.get("reference_column").and_then(|v| v.as_str()).unwrap_or("");
                let pred = s.get("prediction_column").and_then(|v| v.as_str()).unwrap_or("prediction");
                json!({"query": format!(
                    "SELECT COUNT(*) FILTER (WHERE \"{}\" = \"{}\") * 1.0 / COUNT(*) AS accuracy FROM input_0",
                    target, pred)})
            });
        def!("org.knime.base.node.io.pmml.PMMLWriterNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/model.json");
                json!({"query": format!("SELECT * FROM ml_load_xgboost('exported_model', '{}')", path)})
            });
        def!("org.knime.base.node.io.pmml.PMMLReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/model.json");
                json!({"query": format!("SELECT * FROM ml_load_xgboost('imported_model', '{}')", path)})
            });


        // ─── Analysis — Missing duckdb-ml algorithms ───
        def!("org.knime.base.node.mine.knn.KNNNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols = feats.iter().map(|f| format!("\"{}\"", f)).collect::<Vec<_>>();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                let k = s.get("number_of_neighbors").and_then(|v| v.as_str()).unwrap_or("5");
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_knn', 'knn_regressor', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"k\": {}}}')",
                    target, feat_json_array, k)})
            });
        def!("org.knime.base.node.mine.lasso.LassoNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols = feats.iter().map(|f| format!("\"{}\"", f)).collect::<Vec<_>>();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                let alpha = s.get("alpha").and_then(|v| v.as_str()).unwrap_or("1.0");
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_lasso', 'lasso_regression', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"lambda\": {}}}')",
                    target, feat_json_array, alpha)})
            });

        // ─── DuckDB WASM extension bridge (ducklink) ───
        def!("org.knime.base.node.io.filehandling.node.WebAppNodeFactory", "core.sql",
            |s| {
                let component = s.get("wasm_component").and_then(|v| v.as_str()).unwrap_or("my_extension");
                json!({"query": format!("FROM ducklink_load('{}'); SELECT * FROM ducklink.modules", component)})
            });

        // ─── SAS / SPSS / Stata readers (via DuckDB community extensions) ───
        def!("org.knime.base.node.io.filehandling.node.SASReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_csv_auto('{}')", path)})
            });
        def!("org.knime.base.node.io.filehandling.node.SPSSReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_parquet('{}')", path)})
            });

        // ─── DB Query / Table Selector / Connection ───
        def!("org.knime.base.node.io.database.DBQueryReaderNodeFactory", "core.sql",
            |s| json!({"query": s.get("sql_string").and_then(|v| v.as_str()).unwrap_or("SELECT 1")}));
        def!("org.knime.base.node.io.database.DBTableSelectorNodeFactory", "core.sql",
            |s| json!({"query": format!("SELECT * FROM {}",
                s.get("table_name").and_then(|v| v.as_str()).unwrap_or("unknown_table"))}));
        def!("org.knime.base.node.io.database.DBConnectionNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '{}' AS knime_db (TYPE postgres)",
                s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""))}));


        // ─── Federated DB Connectors (DuckDB ATTACH extensions) ───
        def!("org.knime.base.node.io.database.DBConnectorMySQLNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '{}' AS mysql_db (TYPE mysql)",
                s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""))}));
        def!("org.knime.base.node.io.database.DBConnectorPostgreSQLNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '{}' AS pg_db (TYPE postgres)",
                s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""))}));
        def!("org.knime.base.node.io.database.DBConnectorSQLiteNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '{}' AS sqlite_db (TYPE sqlite)",
                s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""))}));
        def!("org.knime.base.node.io.database.DBConnectorMSSQLNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '{}' AS mssql_db (TYPE sqlserver)",
                s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""))}));
        def!("org.knime.base.node.io.database.DBConnectorMongoDBNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '{}' AS mongo_db (TYPE mongodb)",
                s.get("connection_string").and_then(|v| v.as_str()).unwrap_or(""))}));
        def!("org.knime.base.node.io.database.DBConnectorSalesforceNodeFactory", "core.sql",
            |s| json!({"query": format!("ATTACH '' AS sf (TYPE salesforce)",
                )}));

        // ─── Spatial / Geo (DuckDB spatial extension) ───
        def!("org.knime.base.node.io.spatial.SPATIALReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM ST_Read('{}')", path)})
            });
        def!("org.knime.base.node.io.spatial.GeoJSONReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM ST_Read('{}')", path)})
            });
        def!("org.knime.base.node.io.spatial.ShapeFileReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM ST_Read('{}')", path)})
            });
        def!("org.knime.geospatial.nodes.coordinate.CoordinateTransformNodeFactory", "core.sql",
            |s| {
                let col = s.get("geometry_column").and_then(|v| v.as_str()).unwrap_or("");
                let from_srid = s.get("source_crs").and_then(|v| v.as_str()).unwrap_or("4326");
                let to_srid = s.get("target_crs").and_then(|v| v.as_str()).unwrap_or("3857");
                json!({"query": format!("SELECT *, ST_Transform({}, 'EPSG:{}', 'EPSG:{}') AS {}_transformed FROM input_0", col, from_srid, to_srid, col)})
            });
        def!("org.knime.geospatial.nodes.buffer.BufferNodeFactory", "core.sql",
            |s| {
                let col = s.get("geometry_column").and_then(|v| v.as_str()).unwrap_or("");
                let dist = s.get("distance").and_then(|v| v.as_str()).unwrap_or("100");
                json!({"query": format!("SELECT *, ST_Buffer({}, {}) AS {}_buffered FROM input_0", col, dist, col)})
            });
        def!("org.knime.geospatial.nodes.intersection.IntersectionNodeFactory", "core.sql",
            |s| {
                let col1 = s.get("geometry_column_1").and_then(|v| v.as_str()).unwrap_or("");
                let col2 = s.get("geometry_column_2").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, ST_Intersection({}, {}) AS intersection FROM input_0", col1, col2)})
            });

        // ─── Faker / Test Data (DuckDB faker extension) ───
        def!("org.knime.base.node.util.sampledata.SampleDataNodeFactory", "core.sql",
            |s| {
                let rows = s.get("sample_size").and_then(|v| v.as_str()).unwrap_or("100");
                json!({"query": format!("SELECT * FROM faker('fake', {})", rows)})
            });

        // ─── OData / GraphQL reader (erpl-web extension) ───
        def!("org.knime.base.node.io.odata.ODataReaderNodeFactory", "core.sql",
            |s| {
                let url = s.get("service_url").and_then(|v| v.as_str()).unwrap_or("");
                let entity = s.get("entity_set").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_json_auto('{}/{}')", url, entity)})
            });

        // ─── FTP / SFTP reader (cloudfs) ───
        def!("org.knime.base.node.io.filehandling.FTPFileReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_csv_auto('sftp://{}')", path)})
            });

        // ─── Scripting ───
        def!("org.knime.base.node.preproc.r.RSnippetNodeFactory", "core.script.python",
            |s| json!({"script": s.get("r_script").and_then(|v| v.as_str()).unwrap_or(""), "type": "r"}));
        def!("org.knime.base.node.preproc.java.JavaSnippetNodeFactory", "core.script.python",
            |s| json!({"script": s.get("java_script").and_then(|v| v.as_str()).unwrap_or(""), "type": "java"}));
        def!("org.knime.base.node.preproc.javaedit.JavaEditVariableNodeFactory", "core.script.python",
            |s| json!({"script": s.get("java_script").and_then(|v| v.as_str()).unwrap_or(""), "type": "java_variable"}));

        // ─── Image / Binary ───
        def!("org.knime.base.node.viz.image.ImagePortObjectToImageNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.io.imgwriter.ImageWriterNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/output.png");
                json!({"query": format!("COPY (SELECT * FROM input_0) TO '{}'", path)})
            });
        def!("org.knime.base.node.io.imgreader.ImageReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_blob('{}')", path)})
            });

        // ─── Viz — Classic Views (core.sql passthrough) ───
        def!("org.knime.base.views.node.lineplot.LinePlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.scatterplot.ScatterPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.piechart.PieChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.histogram.HistogramNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.boxplot.BoxPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.heatmap.HeatMapNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Viz — Plotter variants ───
        def!("org.knime.base.node.viz.plotter.line.LinePlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.scatter.ScatterPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.pie.PieChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.histogram.HistogramNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.box.BoxPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.scattermatrix.ScatterMatrixNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.parallelcoordinates.ParallelCoordinatesNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.heatmap.HeatMapNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.sunburst.SunburstNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.histogram.HistogramNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.radviz.RadVizNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.tagcloud.TagCloudNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Viz — JS (Modern) views ───
        def!("org.knime.js.base.node.viz.scatter.ScatterPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.line.LinePlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.bar.BarChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.pie.PieChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.histogram.HistogramNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.box.BoxPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.table.TableViewNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.heatmap.HeatMapNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.sunburst.SunburstNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.parallelcoordinates.ParallelCoordinatesNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.scatterplotmatrix.ScatterPlotMatrixNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.tile.TileViewNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.radviz.RadVizNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.tagcloud.TagCloudNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.lift.LiftChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.roc.ROCNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.network.NetworkGraphNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Viz — Color / Shape / Size Manager ───
        def!("org.knime.base.node.viz.property.color.ColorManagerNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.property.shape.ShapeManagerNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.property.size.SizeManagerNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── IO — Model / ARFF / Parquet ───
        def!("org.knime.base.node.io.filehandling.ARFFReaderNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""), "format": "arff"}));
        def!("org.knime.base.node.io.filehandling.ARFFWriterNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/output.arff");
                json!({"query": format!("COPY (SELECT * FROM input_0) TO '{}'", path)})
            });
        def!("org.knime.base.node.io.table.writer.TableWriterNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.base.node.io.parquet.ParquetReaderNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""), "format": "parquet"}));
        def!("org.knime.base.node.io.parquet.ParquetWriterNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""), "format": "parquet"}));

        // ─── Column Combiner / Splitter ───
        def!("org.knime.base.node.preproc.colcombine.ColumnCombinerNodeFactory", "core.sql",
            |s| {
                let cols_str = s.get("included_column_names").and_then(|v| v.as_str()).unwrap_or("");
                let cols: Vec<&str> = cols_str.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).collect();
                let sep = s.get("separator").and_then(|v| v.as_str()).unwrap_or("_");
                let concat = cols.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(&format!(" || '{}' || ", sep));
                json!({"query": format!("SELECT *, {} AS combined FROM input_0", concat)})
            });

        // ─── Nominal / Category nodes ───
        def!("org.knime.base.node.preproc.category.NumberToCategoryNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let bins_str = s.get("bins").and_then(|v| v.as_str()).unwrap_or("");
                let bins: Vec<&str> = bins_str.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).collect();
                let cases: Vec<String> = bins.iter().enumerate().map(|(i, b)| {
                    if i == 0 { format!("WHEN \"{}\" <= {} THEN 'cat_{}'", col, b, i) }
                    else { format!("WHEN \"{}\" <= {} THEN 'cat_{}'", col, b, i) }
                }).collect();
                json!({"query": format!("SELECT *, CASE {} ELSE 'cat_{}' END AS category FROM input_0", cases.join(" "), bins.len())})
            });
        def!("org.knime.base.node.preproc.category.CategoryToNumberNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CAST(\"{}\" AS DOUBLE) AS {}_num FROM input_0", col, col)})
            });
        def!("org.knime.base.node.preproc.category.CategoryToClassNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, \"{}\" AS class FROM input_0", col)})
            });

        // ─── Row Splitter (split by count or percentage) ───
        def!("org.knime.base.node.preproc.partition.PartitionNodeFactory", "core.sql",
            |s| {
                let pct = s.get("percentage").and_then(|v| v.as_str()).unwrap_or("70");
                json!({"query": format!("SELECT *, CASE WHEN ROW_NUMBER() OVER () % 100 < {} THEN 'train' ELSE 'test' END AS _partition FROM input_0", pct)})
            });

        // ─── Interactive Widgets ───
        def!("org.knime.js.base.node.widget.filter.nominal.NominalFilterWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "nominal filter widget"}));
        def!("org.knime.js.base.node.widget.filter.range.RangeFilterWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "range filter widget"}));
        def!("org.knime.js.base.node.widget.filter.slider.SliderFilterWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "slider filter widget"}));
        def!("org.knime.js.base.node.widget.selection.column.ColumnSelectionWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "column selection widget"}));
        def!("org.knime.js.base.node.widget.selection.single.SingleSelectionWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "single selection widget"}));
        def!("org.knime.js.base.node.widget.selection.multiple.MultipleSelectionWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "multiple selection widget"}));
        def!("org.knime.js.base.node.widget.button.ButtonWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "button widget"}));
        def!("org.knime.js.base.node.widget.text.TextOutputWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "text output widget"}));
        def!("org.knime.js.base.node.widget.image.ImageOutputWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "image output widget"}));
        def!("org.knime.js.base.node.widget.file.FileDownloadWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file download widget"}));
        def!("org.knime.js.base.node.widget.fileupload.FileUploadWidgetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file upload widget"}));

        // ─── JSON / XML ───
        def!("org.knime.json.node.jsonpathtojson.JsonPathToJsonNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let path = s.get("json_path").and_then(|v| v.as_str()).unwrap_or("$");
                json!({"query": format!("SELECT *, json_extract(\"{}\", '{}') AS json_result FROM input_0", col, path)})
            });
        def!("org.knime.json.node.jsontotable.JsonToTableNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM input_0, LATERAL json_each(\"{}\")", col)})
            });
        def!("org.knime.json.node.tabletojson.TableToJsonNodeFactory", "core.sql",
            |_| json!({"query": "SELECT json_group_object(name, value) AS json_output FROM (SELECT * FROM input_0)"}));
        def_fb!("org.knime.base.node.io.xml.XMLReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_xml('{}')", path)})
            }, "Fallback: SwanFlow Python UDF → lxml.etree");
        def!("org.knime.base.node.io.xml.XMLWriterNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/output.xml");
                json!({"query": format!("COPY (SELECT * FROM input_0) TO '{}'", path)})
            });

        // ─── Google Sheets / BigQuery ───
        def_fb!("org.knime.ext.google.sheets.GoogleSheetsReaderNodeFactory", "core.sql",
            |s| {
                let sid = s.get("sheet_id").and_then(|v| v.as_str()).unwrap_or("");
                let sheet = s.get("sheet_name").and_then(|v| v.as_str()).unwrap_or("Sheet1");
                json!({"query": format!("SELECT * FROM read_csv_auto('gdfs://sheets/{}?sheet={}')", sid, sheet)})
            }, "Fallback: SwanFlow Python UDF → gspread");
        def!("org.knime.ext.google.bigquery.GoogleBigQueryConnectorNodeFactory", "core.sql",
            |s| {
                let project = s.get("project_id").and_then(|v| v.as_str()).unwrap_or("");
                let dataset = s.get("dataset").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM bigquery_scan('{}', '{}', 'SELECT * FROM table')", project, dataset)})
            });

        // ─── Text Mining / NLP ───
        def!("org.knime.ext.textprocessing.nodes.preprocessing.documentvector.DocumentVectorNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, string_split(\"{}\", ' ') AS tokens FROM input_0", col)})
            });
        def!("org.knime.ext.textprocessing.nodes.transformation.stringstodocument.StringsToDocumentNodeFactory", "core.sql",
            |_| json!({"query": "SELECT *, lower(col) AS document FROM input_0"}));
        def!("org.knime.ext.textprocessing.nodes.transformation.documenttostrings.DocumentToStringsNodeFactory", "core.sql",
            |_| json!({"query": "SELECT *, CAST(document AS VARCHAR) AS text FROM input_0"}));
        def!("org.knime.ext.textprocessing.nodes.transformation.tf.TermFrequencyNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT word, COUNT(*) AS freq FROM input_0, LATERAL UNNEST(string_split(\"{}\", ' ')) AS t(word) GROUP BY word ORDER BY freq DESC", col)})
            });
        def!("org.knime.ext.textprocessing.nodes.tagging.tagger.DictionaryTaggerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, \"{}\" AS tag FROM input_0", col)})
            });

        // ─── Deep Learning / Keras ───
        def!("org.knime.dl.keras.base.nodes.layers.DLKerasInputLayerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras input layer → export to ONNX → use ml_load_onnx()"}));
        def!("org.knime.dl.keras.base.nodes.layers.DLKerasDenseLayerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras dense layer → export to ONNX → use ml_load_onnx()"}));
        def!("org.knime.dl.keras.base.nodes.learner.DLKerasNetworkLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras network learner → train in Python, export ONNX → ml_load_onnx()"}));
        def!("org.knime.dl.keras.base.nodes.executor.DLKerasNetworkExecutorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras executor → ml_predict() with ONNX model loaded via ml_load_onnx()"}));

        // ─── Python Script (v2) ───
        def!("org.knime.python3.scripting.nodes.script.PythonScriptNodeFactory2", "core.script.python",
            |s| json!({"script": s.get("python_script").and_then(|v| v.as_str()).unwrap_or(""), "type": "python3"}));

        // ─── Conda / Environment Propagation ───
        def!("org.knime.python3.scripting.nodes.conda.CondaEnvironmentPropagationNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "conda env propagation"}));

        // ─── Miscellaneous ───
        def!("org.knime.base.node.logging.LoggingNodeFactory", "core.sql",
            |s| {
                let msg = s.get("message").and_then(|v| v.as_str()).unwrap_or("log entry");
                json!({"query": format!("SELECT *, '{}' AS log_message, now() AS log_time FROM input_0", msg)})
            });
        def!("org.knime.base.node.util.sendemail.SendEmailNodeFactory", "core.sql",
            |s| {
                let to = s.get("to").and_then(|v| v.as_str()).unwrap_or("");
                let subject = s.get("subject").and_then(|v| v.as_str()).unwrap_or("SwanFlow notification");
                json!({"query": format!("SELECT http_post('https://api.sendgrid.com/v3/mail/send', headers := '{{\"Authorization\": \"Bearer $SENDGRID_API_KEY\"}}', body := '{{\"to\":\"{}\",\"subject\":\"{}\"}}') AS email_result FROM input_0", to, subject)})
            });

        // ─── Analysis ───
        def!("org.knime.base.node.preproc.valcounter.ValueCounterNodeFactory", "core.transform.aggregate",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"group_by": [col], "aggs": [format!("COUNT({})", col)]})
            });
        def!("org.knime.base.node.stats.correlation.LinearCorrelationNodeFactory", "core.transform.profiler",
            |_| json!({"analysis": "correlation"}));
        def_fb!("org.knime.base.node.mine.cluster.kmeans.KMeansNodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let n_clusters = s.get("number_of_clusters").and_then(|v| v.as_str()).unwrap_or("3");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_kmeans', 'kmeans', (SELECT json_group_array(0) FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"k\": {}}}')",
                    feat_json_array, n_clusters)})
            }, "Fallback: SwanFlow Python UDF → sklearn.cluster.KMeans");
        def!("org.knime.base.node.mine.tree.decisiontree2.DecisionTreeNodeFactory2", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_dt', 'decision_tree', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            });
        def_fb!("org.knime.base.node.mine.regression.linear.LinearRegressionNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_linreg', 'linear_regression', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            }, "Fallback: SwanFlow Python UDF → sklearn.linear_model.LinearRegression");
        def!("org.knime.base.node.preproc.pca.PCA2NodeFactory", "core.sql",
            |s| {
                let feats = Self::parse_feats(s, "feature_columns");
                let n_components = s.get("number_of_components").and_then(|v| v.as_str()).unwrap_or("2");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_pca', 'pca', (SELECT json_group_array(0) FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{\"n_components\": {}}}')",
                    feat_json_array, n_components)})
            });
        def!("org.knime.base.node.preproc.normalize.Normalizer2NodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "method": s.get("method").and_then(|v| v.as_str()).unwrap_or("minmax")}));
        def!("org.knime.base.node.preproc.normalize.NormalizerNodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "method": s.get("method").and_then(|v| v.as_str()).unwrap_or("minmax")}));
        def!("org.knime.base.node.preproc.category.number2category.NumberToCategoryNodeFactory2", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "bins": s.get("bins")}));
        def!("org.knime.base.node.preproc.category.category2number.CategoryToNumber2NodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.preproc.category.category2number.CategoryToNumberNodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.mine.distance.matrix.DistanceMatrixNodeFactory", "core.sql",
            |s| {
                let cols_str = s.get("included_column_names").and_then(|v| v.as_str()).unwrap_or("");
                let cols: Vec<&str> = cols_str.split(',').map(|c| c.trim()).filter(|c| !c.is_empty()).collect();
                if cols.len() < 2 {
                    json!({"query": "SELECT 'Need >=2 numeric columns' AS error FROM input_0"})
                } else {
                    let pairs: Vec<String> = (0..cols.len()).flat_map(|i| {
                        let c = cols.clone();
                        ((i+1)..cols.len()).map(move |j| {
                            format!("SQRT(SUM(POWER(\"{}\" - \"{}\" , 2))) AS {}_{}_dist", c[i], c[j], c[i], c[j])
                        })
                    }).collect();
                    json!({"query": format!("SELECT {} FROM input_0", pairs.join(", "))})
                }
            });
        def!("org.knime.base.node.mine.cluster.hierarchical.HierarchicalClusterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "hierarchical clustering (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.association.AssociationRuleLearnerNodeFactory", "core.sql",
            |s| {
                let item_col = s.get("item_column").and_then(|v| v.as_str()).unwrap_or("");
                let txn_col = s.get("transaction_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT a.{} AS antecedent, b.{} AS consequent, COUNT(*) AS support FROM input_0 a JOIN input_0 b ON a.{} = b.{} AND a.{} < b.{} GROUP BY 1, 2 HAVING COUNT(*) >= 2 ORDER BY support DESC",
                    item_col, item_col, txn_col, txn_col, item_col, item_col)})
            });

        // ─── Visualization ───
        def!("org.knime.base.node.viz.plotter.pie.PieChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.piechart.PieChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.line.LineChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.lineplot.LinePlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.scatter.ScatterPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.scatter.ScatterMatrixNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.histogram.HistogramNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.histogram.HistogramChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.box.BoxPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.boxplot.BoxPlotChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.heatmap.HeatMapNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.bubble.BubbleChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.sunburst.SunburstNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.parallel.ParallelCoordinatesNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.density.DensityPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.views.node.scatter.ScatterTableViewNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.radar.RadarPlotNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.tagcloud.TagCloudNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.heatmap.HeatMapJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.scatter.ScatterPlotterNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.viz.plotter.bar.BarChartJFreeChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Row/Table Ops ───
        def!("org.knime.base.node.preproc.row.filter.RowSplitterNodeFactory", "core.transform.filter",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let op = s.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
                let val = s.get("filter_value").and_then(|v| v.as_str()).unwrap_or("");
                json!({"expr": format!("{} {} {}", col, op, val), "split": true})
            });
        def!("org.knime.base.node.preproc.row.split.RowSplitter2NodeFactory", "core.transform.filter",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let op = s.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
                let val = s.get("filter_value").and_then(|v| v.as_str()).unwrap_or("");
                json!({"expr": format!("{} {} {}", col, op, val), "split": true})
            });
        def!("org.knime.base.node.preproc.filter.row.RowSamplingNodeFactory", "core.transform.filter",
            |s| json!({"sample_size": s.get("sample_size"),
                       "seed": s.get("seed"),
                       "method": s.get("sampling_method").and_then(|v| v.as_str()).unwrap_or("random")}));
        def!("org.knime.base.node.preproc.cell.replace.CellReplacerNodeFactory", "core.sql",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "pattern": s.get("pattern").and_then(|v| v.as_str()),
                       "replacement": s.get("replacement").and_then(|v| v.as_str())}));

        // ─── Window ───
        def!("org.knime.base.node.preproc.windowing.WindowingNodeFactory", "core.sql",
            |s| json!({"query": Self::build_window_sql(s)}));

        // ─── Reference Row ───
        def!("org.knime.base.node.preproc.reference.ReferenceRowNodeFactory", "core.sql",
            |_| json!({"query": "SELECT *, ROW_NUMBER() OVER () AS ref_row FROM input_0"}));

        // ─── Extract Column Header ───
        def!("org.knime.base.node.preproc.colheader.ExtractColumnHeaderNodeFactory", "core.transform.project",
            |_| json!({"extract_header": true}));

        // ─── Interactive / Web Views ───
        def!("org.knime.js.base.node.viz.chart.GenericChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.chart.GenericEChartsNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── One to Many ───
        def!("org.knime.base.node.preproc.onetomany.OneToManyNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, UNNEST(string_split(\"{}\", ',')) AS {}_expanded FROM input_0", col, col)})
            });

        // ─── String / Text ───
        def!("org.knime.base.node.preproc.stringreplacer.StringReplacerNodeFactory", "core.sql",
            |s| json!({"query": format!("SELECT *, REPLACE({}, '{}', '{}') AS result FROM input_0",
                s.get("column_name").and_then(|v| v.as_str()).unwrap_or(""),
                s.get("pattern").and_then(|v| v.as_str()).unwrap_or(""),
                s.get("replacement").and_then(|v| v.as_str()).unwrap_or(""))}));
        def!("org.knime.base.node.preproc.string2number.StringToNumberNodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "target_type": "number"}));
        def!("org.knime.base.node.preproc.number2string.NumberToStringNodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "target_type": "string"}));
        def!("org.knime.base.node.preproc.number2string.NumberToString2NodeFactory", "core.transform.project",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "target_type": "string"}));
        def!("org.knime.base.node.preproc.regexsplit.RegexSplitNodeFactory", "core.sql",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str()),
                       "pattern": s.get("pattern").and_then(|v| v.as_str())}));
        def!("org.knime.ext.textprocessing.nodes.transformation.stringstodocument.StringsToDocumentNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.ext.textprocessing.nodes.transformation.documenttostring.DocumentToStringNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Math / Rounding ───
        def!("org.knime.base.node.preproc.rounddouble.RoundDoubleNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let decimals = s.get("decimal_places").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(2);
                json!({"query": format!("SELECT *, ROUND({}, {}) AS rounded FROM input_0", col, decimals)})
            });
        def!("org.knime.base.node.preproc.rounddouble.RoundDouble2NodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, ROUND({}) AS rounded FROM input_0", col)})
            });
        def!("org.knime.base.node.preproc.mathformula.MathFormulaMultiNodeFactory", "core.transform.project",
            |s| json!({"expressions": s.get("formulas")}));

        // ─── Lag / Moving Average / Binning ───
        def!("org.knime.base.node.preproc.lagcolumn.LagColumnNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let lag = s.get("lag").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(1);
                json!({"query": format!("SELECT *, LAG({}, {}) OVER () AS lag_{} FROM input_0", col, lag, col)})
            });
        def!("org.knime.base.node.preproc.movingaverage.MovingAverageNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("*");
                let window = s.get("window_size").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(3);
                json!({"query": format!("SELECT *, AVG({}) OVER (ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS ma_{} FROM input_0", col, window - 1, col)})
            });
        def!("org.knime.base.node.preproc.binner.BinnerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let bins = s.get("number_of_bins").and_then(|v| v.as_str()).and_then(|v| v.parse::<u32>().ok()).unwrap_or(10);
                json!({"query": format!("SELECT *, NTILE({}) OVER (ORDER BY {}) AS bin FROM input_0", bins, col)})
            });
        def!("org.knime.base.node.preproc.binner.AutoBinnerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, WIDTH_BUCKET({}, (SELECT MIN({}) FROM input_0), (SELECT MAX({}) FROM input_0), 10) AS bin FROM input_0", col, col, col)})
            });

        // ─── Column Ops ───
        def!("org.knime.base.node.preproc.columnappender.ColumnAppenderNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.preproc.columnmerger.ColumnMergerNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.preproc.columncomparator.ColumnComparatorNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.base.node.preproc.columnexpand.ColumnExpandNodeFactory", "core.sql",
            |s| json!({"column": s.get("column_name").and_then(|v| v.as_str())}));

        // ─── Date / Time ───
        def!("org.knime.time.node.extract.datetime.ExtractDateTimeFieldsNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let fields = s.get("extracted_fields").and_then(|v| v.as_str()).unwrap_or("YEAR,MONTH,DAY");
                json!({"query": format!("SELECT *, EXTRACT(YEAR FROM CAST({} AS DATE)) AS year, EXTRACT(MONTH FROM CAST({} AS DATE)) AS month, EXTRACT(DAY FROM CAST({} AS DATE)) AS day FROM input_0", col, col, col)})
            });
        def!("org.knime.time.node.convert.stringtodatetime.StringToDateTimeNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CAST({} AS TIMESTAMP) AS datetime FROM input_0", col)})
            });
        def!("org.knime.time.node.convert.datetimetostring.DateTimeToStringNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CAST({} AS VARCHAR) AS str FROM input_0", col)})
            });
        def!("org.knime.time.node.shift.datetimeshift.DateTimeShiftNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let shift = s.get("shift_amount").and_then(|v| v.as_str()).unwrap_or("1");
                let unit = s.get("shift_unit").and_then(|v| v.as_str()).unwrap_or("DAY");
                json!({"query": format!("SELECT *, CAST({} AS DATE) + INTERVAL '{}' {} AS shifted FROM input_0", col, shift, unit)})
            });
        def!("org.knime.time.node.manipulate.timeseries.TimeSeriesNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let window = s.get("window_size").and_then(|v| v.as_str()).unwrap_or("7");
                json!({"query": format!("SELECT *, AVG({}) OVER (ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS {}_sma, LAG({}, 1) OVER (ORDER BY 1) AS {}_lag1 FROM input_0", col, window, col, col, col)})
            });

        // ─── Database Write ───
        def!("org.knime.base.node.io.database.DBWriterNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.base.node.io.database.DBTableCreatorNodeFactory", "core.sql",
            |s| json!({"query": format!("CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM input_0",
                s.get("table_name").and_then(|v| v.as_str()).unwrap_or("temp_table"))}));
        def!("org.knime.base.node.io.database.DBInsertNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "mode": "insert"}));
        def!("org.knime.base.node.io.database.DBDeleteNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "mode": "delete"}));
        def!("org.knime.base.node.io.database.DBUpdateNodeFactory", "core.io.db_write",
            |s| json!({"connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or(""),
                       "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
                       "mode": "update"}));

        // ─── File / Network ───
        def_fb!("org.knime.base.node.io.filestore.FileDownloadNodeFactory", "core.sql",
            |s| {
                let url = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_csv_auto('{}')", url)})
            }, "Fallback: SwanFlow Python UDF → requests.get()");
        def!("org.knime.base.node.io.filestore.FileUploadNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/output.parquet");
                json!({"query": format!("COPY (SELECT * FROM input_0) TO '{}'", path)})
            });
        def!("org.knime.base.node.io.filestore.ListFilesNodeFactory", "core.transform.project",
            |s| json!({"path": s.get("directory").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.io.http.HttpRetrieverNodeFactory", "core.sql",
            |s| {
                let url = s.get("url").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_json_auto('{}')", url)})
            });

        // ─── Flow Control ───
        def!("org.knime.base.node.meta.switchcase.SwitchCaseNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT *, CASE {} END AS switch_result FROM input_0", col)})
            });
        def!("org.knime.base.node.meta.trycatch.TryCatchNodeFactory", "core.sql",
            |_| json!({"query": "SELECT *, COALESCE(TRY_CAST(col AS DOUBLE), 0) AS safe_col FROM input_0"}));
        def!("org.knime.base.node.flowcontrol.ifswitch.IFSwitchNodeFactory", "core.sql",
            |s| {
                let cond = s.get("condition").and_then(|v| v.as_str()).unwrap_or("1=1");
                json!({"query": format!("SELECT *, CASE WHEN {} THEN 'branch_a' ELSE 'branch_b' END AS if_branch FROM input_0", cond)})
            });
        def!("org.knime.base.node.meta.timer.TimerInfoNodeFactory", "core.sql",
            |_| json!({"query": "SELECT now() AS execution_time, * FROM input_0"}));
        def!("org.knime.ext.ssh.SSHConnectionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SSH connection → use ducklink WASM component or SwanFlow Python UDF with paramiko"}));

        // ─── Statistics (duckdb-ml + stats_duck powered) ───
        def_fb!("org.knime.base.node.stats.ttest.PairedTTestNodeFactory", "core.sql",
            |s| {
                let col1 = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let col2 = s.get("second_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_ttest_paired(\"{}\", \"{}\") AS t_test FROM input_0", col1, col2)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.ttest_ind / ttest_rel");
        def_fb!("org.knime.base.node.stats.ttest.IndependentTTestNodeFactory", "core.sql",
            |s| {
                let col1 = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let col2 = s.get("second_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_ttest_independent(\"{}\", \"{}\") AS t_test FROM input_0", col1, col2)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.ttest_ind / ttest_rel");
        def_fb!("org.knime.base.node.stats.anova.OneWayANOVANodeFactory", "core.sql",
            |s| {
                let group_col = s.get("group_column").and_then(|v| v.as_str()).unwrap_or("");
                let value_col = s.get("value_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_anova(\"{}\", \"{}\") AS anova FROM input_0", group_col, value_col)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.f_oneway");
        def_fb!("org.knime.base.node.stats.shapiro.ShapiroWilkNodeFactory", "core.sql",
            |s| {
                let col = s.get("included_column_names").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_shapiro_wilk(\"{}\") AS shapiro_wilk FROM input_0", col)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.shapiro");
        def_fb!("org.knime.base.node.stats.chisquare.ChiSquareNodeFactory", "core.sql",
            |s| {
                let col1 = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let col2 = s.get("second_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT stats_duck_chisq(\"{}\", \"{}\") AS chi_square FROM input_0", col1, col2)})
            }, "Fallback: SwanFlow Python UDF → scipy.stats.chi2_contingency");

        // ─── ML (duckdb-ml powered) ───
        def!("org.knime.base.node.mine.bayes.naivebayes.NaiveBayesNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_nb', 'naive_bayes', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            });
        def!("org.knime.base.node.mine.tree.randomforest.RandomForestNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_rf', 'random_forest', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            });
        def!("org.knime.base.node.mine.svm.SVMNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SVM (not in duckdb-ml; use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.neural.mlp.MLPNodeFactory", "core.sql",
            |s| {
                let target = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("");
                let feats = Self::parse_feats(s, "feature_columns");
                let feat_cols: Vec<String> = feats.iter().map(|f| format!("\"{}\"", f)).collect();
                let feat_json_array = format!("json_array({})", feat_cols.join(", "));
                json!({"query": format!(
                    "SELECT * FROM ml_train('sw_mlp', 'mlp_regressor', (SELECT json_group_array(\"{}\") FROM input_0), (SELECT json_group_array({}) FROM input_0), '{{}}')",
                    target, feat_json_array)})
            });

        // ─── JS Viz ───
        def!("org.knime.js.base.node.viz.line.LinePlotJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.scatter.ScatterPlotJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.parallel.ParallelCoordinatesJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.bar.BarChartJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.pie.PieChartJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.histogram.HistogramJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.box.BoxPlotJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.bubble.BubbleChartJSNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── Script ───
        def!("org.knime.ext.r.nodes.RSnippetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "R script (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.preproc.javasnippet.JavaSnippetNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "java snippet (use SwanFlow Python UDF)"}));


        // ─── Graph / Network Analysis (DuckDB duckpgq extension) ───
        def!("org.knime.base.node.mine.graph.GraphAnalyzerNodeFactory", "core.sql",
            |s| {
                let edge_table = s.get("edge_table").and_then(|v| v.as_str()).unwrap_or("edges");
                let src = s.get("source_column").and_then(|v| v.as_str()).unwrap_or("src");
                let tgt = s.get("target_column").and_then(|v| v.as_str()).unwrap_or("tgt");
                json!({"query": format!(
                    "CREATE PROPERTY GRAPH knime_graph VERTEX TABLES (SELECT DISTINCT {} AS id FROM {}) EDGE TABLES (SELECT {} AS src, {} AS dst FROM {})",
                    src, edge_table, src, tgt, edge_table)})
            });
        def!("org.knime.base.node.mine.graph.ShortestPathNodeFactory", "core.sql",
            |s| {
                let graph = s.get("graph_name").and_then(|v| v.as_str()).unwrap_or("knime_graph");
                let start = s.get("start_node").and_then(|v| v.as_str()).unwrap_or("0");
                let end = s.get("end_node").and_then(|v| v.as_str()).unwrap_or("0");
                json!({"query": format!(
                    "SELECT * FROM GRAPH_TABLE({} MATCH (a)-[e* SHORTEST 1]->(b) WHERE a.id = {} AND b.id = {} COLUMNS (a.id, b.id, SUM(e.weight)))", 
                    graph, start, end)})
            });
        def!("org.knime.base.node.mine.graph.PageRankNodeFactory", "core.sql",
            |s| {
                let graph = s.get("graph_name").and_then(|v| v.as_str()).unwrap_or("knime_graph");
                json!({"query": format!(
                    "SELECT * FROM GRAPH_TABLE({} MATCH (a)-[e]->(b) COLUMNS (a.id, b.id, COUNT(*)))",
                    graph)})
            });

        // ─── Remote Access (quack / harbor) ───
        def!("org.knime.base.node.io.remote.RemoteExecutorNodeFactory", "core.sql",
            |s| {
                let host = s.get("host").and_then(|v| v.as_str()).unwrap_or("localhost");
                let port = s.get("port").and_then(|v| v.as_str()).unwrap_or("5432");
                let query = s.get("sql_query").and_then(|v| v.as_str()).unwrap_or("SELECT 1");
                json!({"query": format!(
                    "ATTACH 'host={} port={}' AS remote_duckdb (TYPE quack); SELECT * FROM remote_duckdb.({})",
                    host, port, query)})
            });
        def!("org.knime.base.node.io.remote.RemoteFileReaderNodeFactory", "core.sql",
            |s| {
                let url = s.get("url").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM '{}'", url)})
            });

        // ─── Additional KNIME base nodes ───
        def!("org.knime.base.node.preproc.outlier.OutlierDetectorNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT *, CASE WHEN ABS({0} - AVG({0}) OVER()) > 3 * STDDEV({0}) OVER() THEN 'outlier' ELSE 'normal' END AS outlier_flag FROM input_0",
                    col)})
            });
        def!("org.knime.base.node.preproc.discretize.DiscretizeNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let bins = s.get("number_of_bins").and_then(|v| v.as_str()).unwrap_or("5");
                json!({"query": format!(
                    "SELECT *, WIDTH_BUCKET({0}, MIN({0}) OVER(), MAX({0}) OVER() + 1, {1}) AS {0}_bin FROM input_0",
                    col, bins)})
            });
        def!("org.knime.base.node.preproc.one2many.CellSplitterNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let delim = s.get("delimiter").and_then(|v| v.as_str()).unwrap_or(",");
                json!({"query": format!("SELECT *, UNNEST(string_split(\"{}\", '{}')) AS split_value FROM input_0", col, delim)})
            });
        def!("org.knime.base.node.preproc.missingval.MissingValueReplacerNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let val = s.get("replacement_value").and_then(|v| v.as_str()).unwrap_or("0");
                json!({"query": format!("SELECT *, COALESCE(\"{}\", {}) AS {}_filled FROM input_0", col, val, col)})
            });
        def!("org.knime.base.node.util.memory.MemoryMonitorNodeFactory", "core.sql",
            |_| json!({"query": "SELECT duckdb_memory() AS memory_usage, * FROM input_0"}));



        // ─── ADBC Scanner (Arrow Database Connectivity) ───
        def!("org.knime.base.node.io.database.ADBCConnectorNodeFactory", "core.sql",
            |s| {
                let driver = s.get("driver").and_then(|v| v.as_str()).unwrap_or("duckdb");
                let uri = s.get("connection_uri").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "ATTACH '{}' AS adbc_source (TYPE adbc, DRIVER '{}')", uri, driver)})
            });

        // ─── DuckDB AI (LLM / Embedding in SQL) ───
        def!("org.knime.base.node.mine.ai.LLMPromptNodeFactory", "core.sql",
            |s| {
                let prompt = s.get("prompt").and_then(|v| v.as_str()).unwrap_or("Summarize this");
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let model = s.get("model").and_then(|v| v.as_str()).unwrap_or("gpt-4o");
                json!({"query": format!(
                    "SELECT *, prompt('{}', {}) AS ai_response FROM input_0", model, col)})
            });
        def!("org.knime.base.node.mine.ai.EmbeddingNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let model = s.get("embedding_model").and_then(|v| v.as_str()).unwrap_or("text-embedding-3-small");
                json!({"query": format!(
                    "SELECT *, embed('{}', {}) AS embedding FROM input_0", model, col)})
            });
        def!("org.knime.base.node.mine.ai.VectorSearchNodeFactory", "core.sql",
            |s| {
                let query = s.get("search_query").and_then(|v| v.as_str()).unwrap_or("");
                let col = s.get("embedding_column").and_then(|v| v.as_str()).unwrap_or("");
                let k = s.get("top_k").and_then(|v| v.as_str()).unwrap_or("10");
                json!({"query": format!(
                    "SELECT *, array_cosine_similarity({}, embed('text-embedding-3-small', '{}')) AS similarity FROM input_0 ORDER BY similarity DESC LIMIT {}",
                    col, query, k)})
            });

        // ─── flockmtl (LLM + RAG bridge) ───
        def!("org.knime.base.node.mine.ai.LLMRAGNodeFactory", "core.sql",
            |s| {
                let prompt = s.get("prompt").and_then(|v| v.as_str()).unwrap_or("Answer based on context");
                let context = s.get("context_column").and_then(|v| v.as_str()).unwrap_or("");
                let model = s.get("model").and_then(|v| v.as_str()).unwrap_or("gpt-4o");
                json!({"query": format!(
                    "SELECT *, llm_complete('{}', {} || '\n\n' || '{}') AS rag_response FROM input_0",
                    model, context, prompt)})
            });

        // ═══════════════════════════════════════════════════════════════
        // Round 10 — New DuckDB extensions: excel, azure, iceberg, delta,
        // avro, odbc, vss, yaml, pdf, gcs, unity_catalog, lance, vortex,
        // sheetreader, h3, tdigest, read_stat
        // ═══════════════════════════════════════════════════════════════

        // ─── Excel (.xlsx) — duckdb excel extension ───
        // Replaces previous read_csv_auto hacks for Excel nodes
        def_fb!("org.knime.ext.poi3.node.io.filehandling.excel.reader.ExcelTableReaderNodeFactory2", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                let sheet = s.get("sheet_name").and_then(|v| v.as_str()).unwrap_or("Sheet1");
                json!({"query": format!(
                    "SELECT * FROM st_read('{}', layer='{}')", path, sheet)})
            }, "Fallback: DuckDB excel extension → read_xlsx()");

        def!("org.knime.ext.poi3.node.io.filehandling.excel.writer.ExcelTableWriterNodeFactory2", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("/tmp/output.xlsx");
                json!({"query": format!(
                    "COPY (SELECT * FROM input_0) TO '{}' (FORMAT xlsx)", path)})
            });

        def!("org.knime.base.node.io.filehandling.ExcelReaderNodeFactory2", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                let sheet = s.get("sheet_name").and_then(|v| v.as_str()).unwrap_or("Sheet1");
                json!({"query": format!(
                    "SELECT * FROM st_read('{}', layer='{}')", path, sheet)})
            });

        // ─── Excel (community) — sheetreader / rusty_sheet / spxlsx ───
        def!("org.knime.base.node.io.filehandling.XLSXReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                let sheet = s.get("sheet_name").and_then(|v| v.as_str()).unwrap_or("Sheet1");
                json!({"query": format!(
                    "SELECT * FROM sheetreader('{}', sheet='{}')", path, sheet)})
            });

        def!("org.knime.ext.sharepoint.SharePointExcelReaderNodeFactory", "core.sql",
            |s| {
                let site = s.get("site_url").and_then(|v| v.as_str()).unwrap_or("");
                let file = s.get("file_path").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM spxlsx('{}', file='{}')", site, file)})
            });

        // ─── Azure Blob Storage (duckdb azure extension) ───
        def!("org.knime.base.node.io.cloud.AzureBlobConnectorNodeFactory", "core.sql",
            |s| {
                let container = s.get("container").and_then(|v| v.as_str()).unwrap_or("");
                let account = s.get("account_name").and_then(|v| v.as_str()).unwrap_or("");
                let path = s.get("blob_path").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM 'azure://{}/{}/{}'", account, container, path)})
            });

        def!("org.knime.base.node.io.cloud.AzureBlobReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM read_csv_auto('azure://{}')", path)})
            });

        // ─── GCS (Google Cloud Storage) ───
        def!("org.knime.base.node.io.cloud.GCSConnectorNodeFactory", "core.sql",
            |s| {
                let bucket = s.get("bucket").and_then(|v| v.as_str()).unwrap_or("");
                let path = s.get("object_path").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM 'gcs://{}/{}'", bucket, path)})
            });

        def!("org.knime.base.node.io.cloud.GCSReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM read_parquet('gcs://{}')", path)})
            });

        // ─── Iceberg (Apache Iceberg) — duckdb iceberg extension ───
        def!("org.knime.base.node.io.lake.IcebergReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("table_path").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM iceberg_scan('{}')", path)})
            });

        def!("org.knime.base.node.io.lake.IcebergWriterNodeFactory", "core.sql",
            |s| {
                let path = s.get("table_path").and_then(|v| v.as_str()).unwrap_or("/tmp/iceberg_table");
                json!({"query": format!(
                    "COPY (SELECT * FROM input_0) TO '{}' (FORMAT iceberg)", path)})
            });

        // ─── Delta Lake — duckdb delta extension ───
        def!("org.knime.base.node.io.lake.DeltaReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("table_path").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM delta_scan('{}')", path)})
            });

        def!("org.knime.base.node.io.lake.DeltaWriterNodeFactory", "core.sql",
            |s| {
                let path = s.get("table_path").and_then(|v| v.as_str()).unwrap_or("/tmp/delta_table");
                json!({"query": format!(
                    "COPY (SELECT * FROM input_0) TO '{}' (FORMAT delta)", path)})
            });

        // ─── Lance columnar format — duckdb lance extension ───
        def!("org.knime.base.node.io.lake.LanceReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM '{}'", path)})
            });

        // ─── Unity Catalog — duckdb unity_catalog extension ───
        def!("org.knime.base.node.io.catalog.UnityCatalogReaderNodeFactory", "core.sql",
            |s| {
                let endpoint = s.get("endpoint").and_then(|v| v.as_str()).unwrap_or("");
                let catalog = s.get("catalog").and_then(|v| v.as_str()).unwrap_or("main");
                let schema = s.get("schema").and_then(|v| v.as_str()).unwrap_or("default");
                let table = s.get("table").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "ATTACH '{}' AS uc (TYPE unity_catalog); SELECT * FROM uc.{}.{}.{}",
                    endpoint, catalog, schema, table)})
            });

        // ─── Avro — duckdb avro extension ───
        def!("org.knime.base.node.io.filehandling.AvroReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_avro('{}')", path)})
            });

        // ─── ODBC — duckdb odbc extension ───
        def!("org.knime.base.node.io.database.ODBCConnectorNodeFactory", "core.sql",
            |s| {
                let dsn = s.get("dsn").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "ATTACH 'dsn={}' AS odbc_db (TYPE odbc)", dsn)})
            });

        def!("org.knime.base.node.io.database.ODBCReaderNodeFactory", "core.sql",
            |s| {
                let dsn = s.get("dsn").and_then(|v| v.as_str()).unwrap_or("");
                let query = s.get("sql_string").and_then(|v| v.as_str()).unwrap_or("SELECT * FROM table");
                json!({"query": format!(
                    "ATTACH 'dsn={}' AS odbc_src (TYPE odbc); {}",
                    dsn, query)})
            });

        // ─── VSS Vector Similarity Search — duckdb vss extension ───
        def!("org.knime.base.node.mine.similarity.VectorIndexNodeFactory", "core.sql",
            |s| {
                let col = s.get("embedding_column").and_then(|v| v.as_str()).unwrap_or("embedding");
                let table = s.get("table_name").and_then(|v| v.as_str()).unwrap_or("vectors");
                json!({"query": format!(
                    "CREATE INDEX {}_idx ON {} USING HNSW ({})", table, table, col)})
            });

        def!("org.knime.base.node.mine.similarity.VectorSearchNodeFactory", "core.sql",
            |s| {
                let col = s.get("embedding_column").and_then(|v| v.as_str()).unwrap_or("embedding");
                let query_vec = s.get("query_vector").and_then(|v| v.as_str()).unwrap_or("");
                let k = s.get("top_k").and_then(|v| v.as_str()).unwrap_or("10");
                json!({"query": format!(
                    "SELECT *, array_distance({}, {}::FLOAT[128]) AS _distance FROM input_0 ORDER BY _distance LIMIT {}",
                    col, query_vec, k)})
            });

        // ─── YAML — duckdb yaml extension ───
        def_fb!("org.knime.base.node.io.filehandling.YAMLReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_yaml('{}')", path)})
            }, "Fallback: SwanFlow Python UDF → PyYAML");

        // ─── PDF — duckdb pdf extension ───
        def_fb!("org.knime.base.node.io.filehandling.PDFReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_text('pdf://{}')", path)})
            }, "Fallback: SwanFlow Python UDF → PyMuPDF / pdfplumber");

        def!("org.knime.base.node.io.filehandling.PDFTableReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM parquet_scan('{}')", path)})
            });

        // ─── H3 Geospatial Indexing — spatial extension H3 functions ───
        def!("org.knime.geospatial.nodes.h3.H3IndexNodeFactory", "core.sql",
            |s| {
                let lat_col = s.get("latitude_column").and_then(|v| v.as_str()).unwrap_or("lat");
                let lon_col = s.get("longitude_column").and_then(|v| v.as_str()).unwrap_or("lon");
                let resolution = s.get("resolution").and_then(|v| v.as_str()).unwrap_or("8");
                json!({"query": format!(
                    "SELECT *, h3_latlng_to_cell({}, {}, {}) AS h3_cell FROM input_0",
                    lat_col, lon_col, resolution)})
            });

        def!("org.knime.geospatial.nodes.h3.H3AggregateNodeFactory", "core.sql",
            |s| {
                let h3_col = s.get("h3_column").and_then(|v| v.as_str()).unwrap_or("h3_cell");
                let val_col = s.get("value_column").and_then(|v| v.as_str()).unwrap_or("*");
                json!({"query": format!(
                    "SELECT {}, COUNT({}) AS cnt, AVG(CAST({} AS DOUBLE)) AS avg_val FROM input_0 GROUP BY {}",
                    h3_col, val_col, val_col, h3_col)})
            });

        // ─── tdigest — approximate quantiles ───
        def!("org.knime.base.node.stats.quantile.QuantileNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let quantiles = s.get("quantiles").and_then(|v| v.as_str()).unwrap_or("0.25,0.5,0.75,0.95,0.99");
                json!({"query": format!(
                    "SELECT tdigest_quantile({}, [{}]) AS quantiles FROM input_0", col, quantiles)})
            });

        // ─── read_stat — SAS/Stata/SPSS via DuckDB stats_duck ───
        def!("org.knime.base.node.io.filehandling.StataReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_dta('{}')", path)})
            });

        def!("org.knime.base.node.io.filehandling.SAS7BDATReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!("SELECT * FROM read_sas('{}')", path)})
            });

        // ─── Overture Maps — duckdb overture extension ───
        def!("org.knime.geospatial.nodes.overture.OvertureReaderNodeFactory", "core.sql",
            |s| {
                let theme = s.get("theme").and_then(|v| v.as_str()).unwrap_or("places");
                let bbox = s.get("bbox").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM overture('{}', bbox='{}')", theme, bbox)})
            });

        // ─── GeoArrow — duckdb duck_geoarrow extension ───
        def!("org.knime.geospatial.nodes.geoarrow.GeoArrowReaderNodeFactory", "core.sql",
            |s| {
                let path = s.get("file_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT ST_AsText(geometry) AS wkt, * FROM read_parquet('{}')", path)})
            });

        // ─── Harbor — DuckDB HTTP server/client ───
        def!("org.knime.base.node.io.remote.HarborServerNodeFactory", "core.sql",
            |s| {
                let port = s.get("port").and_then(|v| v.as_str()).unwrap_or("42197");
                let token = s.get("auth_token").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "ATTACH 'host=localhost port={} token={}' AS harbor_db (TYPE harbor)",
                    port, token)})
            });

        // ─── GeoIP — duckdb geoip extension ───
        def!("org.knime.base.node.mine.geoip.GeoIPLookupNodeFactory", "core.sql",
            |s| {
                let ip_col = s.get("ip_column").and_then(|v| v.as_str()).unwrap_or("ip_address");
                json!({"query": format!(
                    "SELECT *, geoip_lookup({}) AS geoip FROM input_0", ip_col)})
            });

        // ─── DuckDB MCP — model context protocol bridge ───
        def!("org.knime.base.node.mine.mcp.MCPClientNodeFactory", "core.sql",
            |s| {
                let server = s.get("mcp_server").and_then(|v| v.as_str()).unwrap_or("");
                let tool = s.get("tool_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM mcp_call('{}', '{}', input_0)", server, tool)})
            });

        // ─── OpenTelemetry — duckdb otlp extension ───
        def!("org.knime.base.node.monitor.OTLPReaderNodeFactory", "core.sql",
            |s| {
                let endpoint = s.get("endpoint").and_then(|v| v.as_str()).unwrap_or("http://localhost:4318");
                json!({"query": format!(
                    "SELECT * FROM otlp_traces('{}')", endpoint)})
            });

        // ─── ERPL Web — OData/GraphQL/REST SAP connector ───
        def!("org.knime.base.node.io.erpl.ERPLODataReaderNodeFactory", "core.sql",
            |s| {
                let service = s.get("odata_service").and_then(|v| v.as_str()).unwrap_or("");
                let entity = s.get("entity_set").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT * FROM odata_scan('{}/{}')", service, entity)})
            });

        def!("org.knime.base.node.io.erpl.ERPLGraphQLReaderNodeFactory", "core.sql",
            |s| {
                let endpoint = s.get("graphql_endpoint").and_then(|v| v.as_str()).unwrap_or("");
                let query = s.get("query").and_then(|v| v.as_str()).unwrap_or("{ __typename }");
                json!({"query": format!(
                    "SELECT * FROM graphql_scan('{}', '{}')", endpoint, query)})
            });

        // ─── Web Macro / MiniJinja / Tera templating ───
        def!("org.knime.base.node.report.TemplateRendererNodeFactory", "core.sql",
            |s| {
                let template = s.get("template").and_then(|v| v.as_str()).unwrap_or("");
                let col = s.get("context_column").and_then(|v| v.as_str()).unwrap_or("");
                json!({"query": format!(
                    "SELECT *, minijinja_render('{}', {}) AS rendered FROM input_0", template, col)})
            });

        // ─── Data Diff — duckdb duck_diff extension ───
        def!("org.knime.base.node.preproc.diff.DataDiffNodeFactory", "core.sql",
            |s| {
                let pk = s.get("primary_key").and_then(|v| v.as_str()).unwrap_or("id");
                json!({"query": format!(
                    "SELECT * FROM duck_diff(input_0, input_1, '{}')", pk)})
            });

        // ─── DuckDB Events hook ───
        def!("org.knime.base.node.monitor.DBEventHookNodeFactory", "core.sql",
            |s| {
                let event = s.get("event_type").and_then(|v| v.as_str()).unwrap_or("query_finished");
                let cmd = s.get("command").and_then(|v| v.as_str()).unwrap_or("echo 'query done'");
                json!({"query": format!(
                    "SELECT events_emit('{}', '{}') AS event_result", event, cmd)})
            });

        // ─── Cron jobs in SQL — duckdb cronjob extension ───
        def!("org.knime.base.node.flow.scheduler.SchedulerNodeFactory", "core.sql",
            |s| {
                let schedule = s.get("cron_expression").and_then(|v| v.as_str()).unwrap_or("0 * * * *");
                let query = s.get("sql_query").and_then(|v| v.as_str()).unwrap_or("SELECT now()");
                json!({"query": format!(
                    "SELECT cronjob_schedule('{}', '{}') AS scheduled", schedule, query)})
            });

        // ─── Python UDF in SQL — duckdb python extension ───
        def!("org.knime.base.node.script.PythonSQLUDFNodeFactory", "core.sql",
            |s| {
                let func_name = s.get("function_name").and_then(|v| v.as_str()).unwrap_or("my_udf");
                let py_code = s.get("python_code").and_then(|v| v.as_str()).unwrap_or("return x * 2");
                json!({"query": format!(
                    "CREATE OR REPLACE FUNCTION {} (x) AS '{}' LANGUAGE python; SELECT * FROM input_0",
                    func_name, py_code)})
            });


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
            let mut config = (def.config_fn)(settings);
            // Inject fallback hint if the primary approach uses a DuckDB extension
            if let Some(ref fb) = def.fallback {
                if let Some(obj) = config.as_object_mut() {
                    obj.insert("_fallback".to_string(), serde_json::Value::String(fb.clone()));
                }
            }
            (def.swanflow_type.clone(), config)
        } else {
            ("core.util.unknown".to_string(), serde_json::json!({}))
        }
    }

    #[async_recursion::async_recursion]
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

            let mut instance = NodeInstance {
                id: Uuid::new_v4(),
                type_id: swanflow_type.clone(),
                config: config.clone(),
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

            // If this is a loop metanode, resolve its sub-workflow
            if knime_node.is_metanode && swanflow_type == "core.flow.loop" {
                if let Some(sub_archive) = archive.metanodes.get(&knime_node.id) {
                    match self.convert(sub_archive.clone()).await {
                        Ok(sub_wf) => {
                            let loop_kind = config
                                .get("loop_kind")
                                .and_then(|v| v.as_str())
                                .unwrap_or("generic");

                            // Configure loop parameters from KNIME settings
                            match loop_kind {
                                "chunk" => {
                                    if let Some(size) = config.get("chunk_size").and_then(|v| v.as_u64()) {
                                        instance.max_iterations = size as u32;
                                    }
                                }
                                "counting" | "recursive" => {
                                    if let Some(n) = config.get("iterations")
                                        .or_else(|| config.get("max_depth"))
                                        .and_then(|v| v.as_u64())
                                    {
                                        instance.max_iterations = n as u32;
                                    }
                                }
                                _ => {}
                            }

                            instance.sub_workflow = Some(Box::new(sub_wf));
                            instance.mode = StepMode::Loop;
                            instance.materialize = true; // Collect loop results
                            tracing::info!(
                                "Loop node {} (kind={}, max_iter={}): resolved sub-workflow with {} nodes",
                                &knime_node.id, loop_kind, instance.max_iterations,
                                instance.sub_workflow.as_ref().map(|w| w.graph.node_count()).unwrap_or(0)
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to convert sub-workflow for metanode {}: {}",
                                knime_node.id, e
                            );
                        }
                    }
                }
            }

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

    /// Parse feature columns from KNIME settings (comma-separated string or JSON array).
    fn parse_feats(s: &serde_json::Value, key: &str) -> Vec<String> {
        s.get(key)
            .and_then(|v| {
                v.as_array()
                    .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                    .or_else(|| v.as_str().map(|s| s.split(',').map(|c| c.trim().to_string()).filter(|c| !c.is_empty()).collect()))
            })
            .unwrap_or_default()
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

    /// Build a SQL window query from KNIME windowing settings.
    fn build_window_sql(s: &serde_json::Value) -> String {
        let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("*");
        let agg = s.get("aggregation_method").and_then(|v| v.as_str()).unwrap_or("AVG");
        let partition = s
            .get("partition_columns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let order = s
            .get("order_column")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let frame = s
            .get("window_size")
            .and_then(|v| v.as_str())
            .unwrap_or("UNBOUNDED PRECEDING");

        let mut sql = format!(
            "SELECT *, {}({}) OVER (",
            agg, col
        );
        if !partition.is_empty() {
            sql.push_str(&format!("PARTITION BY {} ", partition));
        }
        if !order.is_empty() {
            sql.push_str(&format!("ORDER BY {} ", order));
        }
        sql.push_str(&format!("ROWS BETWEEN {} AND CURRENT ROW", frame));
        sql.push_str(") AS window_result FROM input_0");
        sql
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
        if factory.contains(".io.") || factory.contains(".filehandling.")
            || factory.contains(".filestore.") || factory.contains(".http.")
        {
            "IO"
        } else if factory.contains(".preproc.") || factory.contains("transform")
            || factory.contains(".rules.") || factory.contains(".formulas")
            || factory.contains(".expressions.")
        {
            "Transform"
        } else if factory.contains("statistics")
            || factory.contains(".stats.")
            || factory.contains("mining")
            || factory.contains(".mine.")
            || factory.contains("cluster")
        {
            "Analysis"
        } else if factory.contains("views")
            || factory.contains(".viz.")
            || factory.contains("plotter")
            || factory.contains("chart")
            || factory.contains(".js.base.")
        {
            "Visualization"
        } else if factory.contains("loop")
            || factory.contains("flowvariable")
            || factory.contains(".variable.")
            || factory.contains(".meta.")
            || factory.contains("collection")
            || factory.contains("flowcontrol")
            || factory.contains("trycatch")
            || factory.contains("switchcase")
        {
            "FlowControl"
        } else if factory.contains("python")
            || factory.contains(".script.")
            || factory.contains(".jep.")
            || factory.contains("snippet")
            || factory.contains(".r.nodes.")
        {
            "Script"
        } else if factory.contains("quickform")
            || factory.contains("dialog")
            || factory.contains("css")
        {
            "UIWidget"
        } else if factory.contains(".time.")
        {
            "Time"
        } else {
            "Other"
        }
    }

    fn compat_level(sw_type: &str) -> &'static str {
        match sw_type {
            "core.missing.placeholder" => "Unsupported",
            "pass_through" => "Passthrough",
            "core.flow.loop" => "Full",
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

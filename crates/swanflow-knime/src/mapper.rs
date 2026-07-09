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
        def!("org.knime.time.node.extract.datetime.ExtractDateTimeFieldNodeFactory", "core.sql",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                let fields = s.get("extracted_fields").and_then(|v| v.as_str()).unwrap_or("year");
                json!({"query": format!("SELECT *, EXTRACT({} FROM {}) AS extracted_{} FROM input_0", fields, col, fields)})
            });
        def!("org.knime.time.node.convert.stringtoduration.StringToDurationNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "string to duration convert"}));
        def!("org.knime.time.node.shift.time.ShiftTimeNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "time shift"}));
        def!("org.knime.time.node.create.datetimerange.DateTimeRangeNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "datetime range create"}));
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
        def!("org.knime.base.node.io.filestore.FileDownloadNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file download"}));
        def!("org.knime.base.node.io.filehandling.node.FileListDirectoryNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file list directory"}));

        // ─── REST / HTTP ───
        def!("org.knime.rest.nodes.GETRequestNodeFactory", "core.missing.placeholder",
            |s| json!({"reason": "REST GET", "url": s.get("url").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.rest.nodes.POSTRequestNodeFactory", "core.missing.placeholder",
            |s| json!({"reason": "REST POST", "url": s.get("url").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.rest.nodes.PUTRequestNodeFactory", "core.missing.placeholder",
            |s| json!({"reason": "REST PUT", "url": s.get("url").and_then(|v| v.as_str()).unwrap_or("")}));

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
        def!("org.knime.base.node.preproc.one2many.OneToManyNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "one to many expand"}));
        def!("org.knime.base.node.preproc.many2one.ManyToOneNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "many to one combine"}));

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
        def!("org.knime.base.node.meta.timedloop.TimedLoopStartNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "timer wait"}));

        // ─── Workflow / SSH ───
        def!("org.knime.base.node.meta.caller.WorkflowExecutorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "workflow executor"}));
        def!("org.knime.ext.ssh.SSHConnectionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "ssh connection"}));
        def!("org.knime.ext.ssh.SSHCommandNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "ssh command"}));

        // ─── Analysis — Regression ───
        def!("org.knime.base.node.mine.regression.linear.LinearRegressionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "linear regression"}));
        def!("org.knime.base.node.mine.regression.logistic.LogisticRegressionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "logistic regression"}));
        def!("org.knime.base.node.mine.regression.polynomial.PolynomialRegressionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "polynomial regression"}));

        // ─── Analysis — Tree / Ensemble ───
        def!("org.knime.base.node.mine.decisiontree2.DecisionTreeLearnerNodeFactory2", "core.missing.placeholder",
            |_| json!({"reason": "decision tree learner"}));
        def!("org.knime.base.node.mine.decisiontree2.DecisionTreePredictorNodeFactory2", "core.missing.placeholder",
            |_| json!({"reason": "decision tree predictor"}));
        def!("org.knime.base.node.mine.treeensemble.randomforest.RandomForestLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "random forest learner"}));
        def!("org.knime.base.node.mine.treeensemble.randomforest.RandomForestPredictorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "random forest predictor"}));
        def!("org.knime.base.node.mine.treeensemble.gradientboost.GradientBoostLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "gradient boost learner"}));

        // ─── Analysis — Clustering ───
        def!("org.knime.base.node.mine.cluster.kmeans.KMeansNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "k-means clustering"}));
        def!("org.knime.base.node.mine.cluster.hierarchical.HierarchicalClusterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "hierarchical clustering"}));
        def!("org.knime.base.node.mine.cluster.dbscan.DBSCANNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "DBSCAN clustering"}));

        // ─── Analysis — Dimension Reduction ───
        def!("org.knime.base.node.mine.pca.PCANodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "PCA"}));

        // ─── Analysis — Statistics ───
        def!("org.knime.base.node.stats.correlation.CorrelationNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "correlation matrix"}));
        def!("org.knime.base.node.stats.shapiro.ShapiroWilkNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "shapiro-wilk normality test"}));
        def!("org.knime.base.node.stats.ttest.TTestNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "t-test"}));
        def!("org.knime.base.node.stats.anova.ANOVANodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "ANOVA"}));
        def!("org.knime.base.node.stats.chisquare.ChiSquareNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "chi-square test"}));

        // ─── Analysis — Naive Bayes / SVM / MLP ───
        def!("org.knime.base.node.mine.bayes.naivebayes.NaiveBayesLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "naive bayes learner"}));
        def!("org.knime.base.node.mine.bayes.naivebayes.NaiveBayesPredictorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "naive bayes predictor"}));
        def!("org.knime.base.node.mine.svm.SVMLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SVM learner"}));
        def!("org.knime.base.node.mine.svm.SVMPredictorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SVM predictor"}));
        def!("org.knime.base.node.mine.mlp.MLPLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "MLP learner"}));
        def!("org.knime.base.node.mine.mlp.MLPPredictorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "MLP predictor"}));

        // ─── Analysis — Scoring / PMML ───
        def!("org.knime.base.node.mine.scorer.accuracy.AccuracyScorerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "accuracy scorer"}));
        def!("org.knime.base.node.io.pmml.PMMLWriterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "PMML writer"}));
        def!("org.knime.base.node.io.pmml.PMMLReaderNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "PMML reader"}));

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
        def!("org.knime.base.node.io.imgwriter.ImageWriterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "image writer"}));
        def!("org.knime.base.node.io.imgreader.ImageReaderNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "image reader"}));

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
        def!("org.knime.base.node.io.filehandling.ARFFWriterNodeFactory", "core.missing.placeholder",
            |s| json!({"reason": "ARFF writer", "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.base.node.io.table.writer.TableWriterNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.base.node.io.parquet.ParquetReaderNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""), "format": "parquet"}));
        def!("org.knime.base.node.io.parquet.ParquetWriterNodeFactory", "core.io.csv_read",
            |s| json!({"path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""), "format": "parquet"}));

        // ─── Column Combiner / Splitter ───
        def!("org.knime.base.node.preproc.colcombine.ColumnCombinerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "column combiner"}));

        // ─── Nominal / Category nodes ───
        def!("org.knime.base.node.preproc.category.NumberToCategoryNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "number to category"}));
        def!("org.knime.base.node.preproc.category.CategoryToNumberNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "category to number"}));
        def!("org.knime.base.node.preproc.category.CategoryToClassNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "category to class"}));

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
        def!("org.knime.json.node.jsonpathtojson.JsonPathToJsonNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "JSON path"}));
        def!("org.knime.json.node.jsontotable.JsonToTableNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "JSON to table"}));
        def!("org.knime.json.node.tabletojson.TableToJsonNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "table to JSON"}));
        def!("org.knime.base.node.io.xml.XMLReaderNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "XML reader"}));
        def!("org.knime.base.node.io.xml.XMLWriterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "XML writer"}));

        // ─── Google Sheets / BigQuery ───
        def!("org.knime.ext.google.sheets.GoogleSheetsReaderNodeFactory", "core.missing.placeholder",
            |s| json!({"reason": "google sheets reader", "sheet_id": s.get("sheet_id").and_then(|v| v.as_str()).unwrap_or("")}));
        def!("org.knime.ext.google.bigquery.GoogleBigQueryConnectorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "Google BigQuery"}));

        // ─── Text Mining / NLP ───
        def!("org.knime.ext.textprocessing.nodes.preprocessing.documentvector.DocumentVectorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "document vector"}));
        def!("org.knime.ext.textprocessing.nodes.transformation.stringstodocument.StringsToDocumentNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "strings to document"}));
        def!("org.knime.ext.textprocessing.nodes.transformation.documenttostrings.DocumentToStringsNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "document to strings"}));
        def!("org.knime.ext.textprocessing.nodes.transformation.tf.TermFrequencyNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "term frequency"}));
        def!("org.knime.ext.textprocessing.nodes.tagging.tagger.DictionaryTaggerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "dictionary tagger"}));

        // ─── Deep Learning / Keras ───
        def!("org.knime.dl.keras.base.nodes.layers.DLKerasInputLayerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras input layer"}));
        def!("org.knime.dl.keras.base.nodes.layers.DLKerasDenseLayerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras dense layer"}));
        def!("org.knime.dl.keras.base.nodes.learner.DLKerasNetworkLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras network learner"}));
        def!("org.knime.dl.keras.base.nodes.executor.DLKerasNetworkExecutorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "keras network executor"}));

        // ─── Python Script (v2) ───
        def!("org.knime.python3.scripting.nodes.script.PythonScriptNodeFactory2", "core.script.python",
            |s| json!({"script": s.get("python_script").and_then(|v| v.as_str()).unwrap_or(""), "type": "python3"}));

        // ─── Conda / Environment Propagation ───
        def!("org.knime.python3.scripting.nodes.conda.CondaEnvironmentPropagationNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "conda env propagation"}));

        // ─── Miscellaneous ───
        def!("org.knime.base.node.logging.LoggingNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "logging"}));
        def!("org.knime.base.node.util.sendemail.SendEmailNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "send email"}));

        // ─── Analysis ───
        def!("org.knime.base.node.preproc.valcounter.ValueCounterNodeFactory", "core.transform.aggregate",
            |s| {
                let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
                json!({"group_by": [col], "aggs": [format!("COUNT({})", col)]})
            });
        def!("org.knime.base.node.stats.correlation.LinearCorrelationNodeFactory", "core.transform.profiler",
            |_| json!({"analysis": "correlation"}));
        def!("org.knime.base.node.mine.cluster.kmeans.KMeansNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "k-means clustering (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.tree.decisiontree2.DecisionTreeNodeFactory2", "core.missing.placeholder",
            |_| json!({"reason": "decision tree (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.regression.linear.LinearRegressionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "linear regression (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.preproc.pca.PCA2NodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "PCA (use SwanFlow Python UDF)"}));
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
        def!("org.knime.base.node.mine.distance.matrix.DistanceMatrixNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "distance matrix (compute with SQL)"}));
        def!("org.knime.base.node.mine.cluster.hierarchical.HierarchicalClusterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "hierarchical clustering (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.association.AssociationRuleLearnerNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "association rules (use SwanFlow Python UDF)"}));

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
        def!("org.knime.base.node.preproc.reference.ReferenceRowNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "reference row filter"}));

        // ─── Extract Column Header ───
        def!("org.knime.base.node.preproc.colheader.ExtractColumnHeaderNodeFactory", "core.transform.project",
            |_| json!({"extract_header": true}));

        // ─── Interactive / Web Views ───
        def!("org.knime.js.base.node.viz.chart.GenericChartNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));
        def!("org.knime.js.base.node.viz.chart.GenericEChartsNodeFactory", "core.sql",
            |_| json!({"query": "SELECT * FROM input_0"}));

        // ─── One to Many ───
        def!("org.knime.base.node.preproc.onetomany.OneToManyNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "one-to-many expansion"}));

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
        def!("org.knime.time.node.manipulate.timeseries.TimeSeriesNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "time series (use DuckDB time functions)"}));

        // ─── Database Write ───
        def!("org.knime.base.node.io.database.DBWriterNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "database write (use SwanFlow DB connector)"}));
        def!("org.knime.base.node.io.database.DBTableCreatorNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "database table create (use SwanFlow DB connector)"}));
        def!("org.knime.base.node.io.database.DBInsertNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "database insert (use SwanFlow DB connector)"}));
        def!("org.knime.base.node.io.database.DBDeleteNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "database delete (use SwanFlow DB connector)"}));
        def!("org.knime.base.node.io.database.DBUpdateNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "database update (use SwanFlow DB connector)"}));

        // ─── File / Network ───
        def!("org.knime.base.node.io.filestore.FileDownloadNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file download"}));
        def!("org.knime.base.node.io.filestore.FileUploadNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "file upload"}));
        def!("org.knime.base.node.io.filestore.ListFilesNodeFactory", "core.transform.project",
            |s| json!({"path": s.get("directory").and_then(|v| v.as_str())}));
        def!("org.knime.base.node.io.http.HttpRetrieverNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "HTTP request (use SwanFlow HttpFetch node)"}));

        // ─── Flow Control ───
        def!("org.knime.base.node.meta.switchcase.SwitchCaseNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "switch/case"}));
        def!("org.knime.base.node.meta.trycatch.TryCatchNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "try/catch"}));
        def!("org.knime.base.node.flowcontrol.ifswitch.IFSwitchNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "if switch"}));
        def!("org.knime.base.node.meta.timer.TimerInfoNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "timer info"}));
        def!("org.knime.ext.ssh.SSHConnectionNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SSH connection"}));

        // ─── Statistics ───
        def!("org.knime.base.node.stats.ttest.PairedTTestNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "paired t-test (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.stats.ttest.IndependentTTestNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "independent t-test (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.stats.anova.OneWayANOVANodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "one-way ANOVA (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.stats.shapiro.ShapiroWilkNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "Shapiro-Wilk test (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.stats.chisquare.ChiSquareNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "chi-square test (use SwanFlow Python UDF)"}));

        // ─── ML ───
        def!("org.knime.base.node.mine.bayes.naivebayes.NaiveBayesNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "naive bayes (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.tree.randomforest.RandomForestNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "random forest (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.svm.SVMNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "SVM (use SwanFlow Python UDF)"}));
        def!("org.knime.base.node.mine.neural.mlp.MLPNodeFactory", "core.missing.placeholder",
            |_| json!({"reason": "neural network MLP (use SwanFlow Python UDF)"}));

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

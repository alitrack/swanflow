use std::collections::HashMap;

use petgraph::stable_graph::NodeIndex;
use serde_json::{json, Value};
use uuid::Uuid;

use swanflow_core::graph::{EdgeInstance, NodeInstance, StepMode, Workflow};
use swanflow_core::nodes::NodeState;

use crate::error::KnimeResult;
use crate::types::KnwfArchive;

/// Maps KNIME node names to SwanFlow native node type_ids.
///
/// Since KNIME `.knwf` only stores `NativeNode`/`SubNode` (not factory class),
/// matching is done by **node display name** with normalized comparison.
pub struct KnimeNodeMapper {
    /// (swanflow_type_id, config_extractor)
    name_map: Vec<(NameMatcher, &'static str, Box<dyn Fn(&Value) -> Value + Send + Sync>)>,
}

enum NameMatcher {
    Exact(&'static str),
    Contains(&'static str),
    StartsWith(&'static str),
}

impl NameMatcher {
    fn matches(&self, name: &str) -> bool {
        match self {
            NameMatcher::Exact(s) => name.eq_ignore_ascii_case(s),
            NameMatcher::Contains(s) => {
                name.to_lowercase().contains(&s.to_lowercase())
            }
            NameMatcher::StartsWith(s) => {
                name.to_lowercase().starts_with(&s.to_lowercase())
            }
        }
    }
}

// KNIME truncates node directory names to ~25 chars.
// Use StartsWith for all name-based matching to handle truncation.
macro_rules! m {
    ($s:expr) => { NameMatcher::StartsWith($s) };
}
macro_rules! contains {
    ($s:expr) => { NameMatcher::Contains($s) };
}

impl KnimeNodeMapper {
    pub fn new() -> Self {
        let mut name_map: Vec<(
            NameMatcher,
            &'static str,
            Box<dyn Fn(&Value) -> Value + Send + Sync>,
        )> = Vec::new();

        // ─── IO: File Readers ───
        name_map.push((m!("CSV Reader"), "core.io.csv_read", Box::new(csv_read_config)));
        name_map.push((m!("CSV Writer"), "core.io.csv_write", Box::new(csv_write_config)));
        name_map.push((contains!("Excel Reader"), "core.io.excel_read", Box::new(excel_read_config)));
        name_map.push((m!("Excel Writer"), "core.io.excel_write", Box::new(excel_write_config)));
        name_map.push((m!("Read Excel Sheet Names"), "core.io.excel_sheet_names", Box::new(passthrough)));
        name_map.push((contains!("Table Reader"), "core.io.universal_read", Box::new(table_read_config)));
        name_map.push((m!("Table Writer"), "core.io.duckdb_write", Box::new(table_write_config)));
        name_map.push((m!("File Reader"), "core.io.universal_read", Box::new(file_read_config)));
        name_map.push((m!("Parquet Reader"), "core.io.parquet_read", Box::new(passthrough)));
        name_map.push((m!("List Files/Folders"), "core.io.universal_read", Box::new(passthrough)));
        name_map.push((m!("Read Images"), "core.io.universal_read", Box::new(passthrough)));

        // ─── IO: Database ───
        name_map.push((m!("DB Reader"), "core.io.db_connector", Box::new(db_read_config)));
        name_map.push((m!("DB Table Selector"), "core.io.db_connector", Box::new(passthrough)));
        name_map.push((m!("MySQL Connector"), "core.io.db_connector", Box::new(passthrough)));
        name_map.push((m!("DuckDB Reader"), "core.io.duckdb_read", Box::new(passthrough)));

        // ─── Transform: Filter ───
        name_map.push((m!("Row Filter"), "core.transform.filter", Box::new(filter_config)));
        name_map.push((m!("Column Filter"), "core.transform.filter", Box::new(column_filter_config)));
        name_map.push((m!("Nominal Value Row Filter"), "core.transform.filter", Box::new(filter_config)));
        name_map.push((contains!("Rule-based Row Filter"), "core.transform.filter", Box::new(filter_config)));
        name_map.push((m!("Duplicate Row Filter"), "core.transform.filter", Box::new(passthrough)));
        name_map.push((m!("Reference Row Filter"), "core.transform.filter", Box::new(passthrough)));
        name_map.push((m!("Row Splitter"), "core.transform.filter", Box::new(filter_config)));
        name_map.push((m!("Row Sampling"), "core.transform.filter", Box::new(passthrough)));

        // ─── Transform: Project/Column ops ───
        name_map.push((m!("Column Renamer"), "core.transform.project", Box::new(column_rename_config)));
        name_map.push((m!("Column Resorter"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Column Splitter"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Column Appender"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Column Merger"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Column Combiner"), "core.transform.project", Box::new(passthrough)));

        // ─── Transform: Aggregate ───
        name_map.push((m!("GroupBy"), "core.transform.aggregate", Box::new(groupby_config)));
        name_map.push((m!("Pivot"), "core.transform.aggregate", Box::new(pivot_config)));
        name_map.push((m!("Unpivot"), "core.transform.aggregate", Box::new(passthrough)));
        name_map.push((m!("Row Aggregator"), "core.transform.aggregate", Box::new(passthrough)));
        name_map.push((m!("Column Aggregator"), "core.transform.aggregate", Box::new(passthrough)));
        name_map.push((m!("Value Counter"), "core.transform.aggregate", Box::new(passthrough)));
        name_map.push((m!("Crosstab"), "core.transform.aggregate", Box::new(passthrough)));

        // ─── Transform: Window ───
        name_map.push((m!("Moving Aggregator"), "core.transform.window", Box::new(passthrough)));
        name_map.push((m!("Rank"), "core.transform.window", Box::new(passthrough)));
        name_map.push((m!("Lag Column"), "core.transform.window", Box::new(passthrough)));
        name_map.push((m!("Moving Average"), "core.transform.window", Box::new(passthrough)));

        // ─── Transform: Join/Union ───
        name_map.push((m!("Joiner"), "core.transform.graph_merger", Box::new(joiner_config)));
        name_map.push((m!("Concatenate"), "core.transform.graph_merger", Box::new(passthrough)));
        name_map.push((m!("Cross Joiner"), "core.transform.graph_merger", Box::new(passthrough)));
        name_map.push((m!("Subset Matcher"), "core.transform.graph_merger", Box::new(passthrough)));
        name_map.push((m!("Table Difference Finder"), "core.transform.graph_merger", Box::new(passthrough)));

        // ─── Transform: Condition/Rule Engine ───
        name_map.push((m!("Rule Engine"), "core.transform.condition", Box::new(rule_engine_config)));
        name_map.push((m!("Rule Engine Variable"), "core.transform.condition", Box::new(rule_engine_config)));
        name_map.push((contains!("CASE Switch"), "core.transform.condition", Box::new(passthrough)));
        name_map.push((m!("End IF"), "core.transform.condition", Box::new(passthrough)));

        // ─── Transform: Math/Formula ───
        name_map.push((m!("Math Formula"), "core.transform.project", Box::new(math_formula_config)));
        name_map.push((contains!("Math Formula"), "core.transform.project", Box::new(math_formula_config)));
        name_map.push((m!("String Manipulation"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Column Expressions"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Constant Value Column"), "core.transform.project", Box::new(constant_column_config)));
        name_map.push((m!("Rule Engine Variable"), "core.transform.project", Box::new(passthrough)));

        // ─── Transform: Sort ───
        name_map.push((m!("Sorter"), "core.transform.window", Box::new(sorter_config)));

        // ─── Quality ───
        name_map.push((m!("Missing Value"), "core.quality.null_check", Box::new(passthrough)));
        name_map.push((m!("Table Validator"), "core.transform.table_schema_validator", Box::new(passthrough)));
        name_map.push((m!("Statistics"), "core.transform.profiler", Box::new(passthrough)));
        name_map.push((m!("Domain Calculator"), "core.transform.profiler", Box::new(passthrough)));
        name_map.push((m!("Data Explorer"), "core.transform.profiler", Box::new(passthrough)));
        name_map.push((m!("Extract Table Dimension"), "core.transform.profiler", Box::new(passthrough)));
        name_map.push((m!("Extract Table Spec"), "core.transform.profiler", Box::new(passthrough)));

        // ─── Generation ───
        name_map.push((m!("Table Creator"), "core.io.csv_read", Box::new(table_creator_config)));
        name_map.push((m!("Data Generator"), "core.io.csv_read", Box::new(passthrough)));
        name_map.push((m!("Create Date/Time Range"), "core.io.csv_read", Box::new(passthrough)));
        name_map.push((m!("Random Numbers Generator"), "core.io.csv_read", Box::new(passthrough)));

        // ─── Row/Table Ops ───
        for &s in &[
            "RowID", "Table Transposer", "Table Cropper", "Table Manipulator",
            "Table Splitter", "Table Updater", "Cell Splitter", "Cell Replacer",
            "String Replacer", "String to Number", "Number to String",
            "One Row to Many", "Many to One", "One to Many",
            "Row to Column Names", "Column Name Extractor", "Column Auto Type Cast",
            "Shuffle", "Counter Generation", "Create Collection Column",
            "Split Collection Column", "Cell Updater", "Cell Splitter By Position",
            "Edit Nominal Domain", "Edit Numeric Domain", "String Cleaner",
            "Number to Category", "Category to Number", "Binner",
            "Round Double", "Reference Column Filter", "Reference Column Splitter",
            "Reference Row Splitter", "Nominal Value Row Splitter",
            "HiLite Row Splitter", "Numeric Row Splitter", "Expression Row Filter",
            "Color Manager", "Shape Manager", "Size Manager", "String Format Manager",
            "Number Format Manager", "Regex Extractor", "Regex Split",
            "Regex Substring", "String Matcher", "String Similarity",
            "Similarity Search", "String Distances", "List To Set", "Set To List",
            "Set Operator", "Collection Size", "Collection to String",
            "Column to Singleton Collection", "Append to Collection",
            "String to Date/Time", "Duration to String", "Extract Duration Fields",
            "Modify Time Zone", "Path to String", "Number to Category (Apply)",
            "Binner (Dictionary)", "String Replacer (Dictionary)",
            "Column Renamer (Dictionary)", "Rule Engine (Dictionary)",
            "Rule-based Row Filter (Dictionary)", "Rule-based Row Splitter (Dictionary)",
            "Cell Extractor", "Column to Grid", "Extract Table Spec",
            "Many to One", "One to Many", "Row Sampling", "Bootstrap Sampling",
            "Equal Size Sampling", "Target Shuffling", "SMOTE",
            "Table Difference Finder", "Table Validator (Reference)",
            "Duplicate Row Filter", "Reference Column Resorter",
        ] {
            name_map.push((m!(s), "core.transform.project", Box::new(passthrough)));
        }

        // ─── Date/Time ───
        name_map.push((m!("Extract Date/Time Fields"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Date/Time Shift"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Date/Time Difference"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Modify Date"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Modify Time"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("String to Date/Time"), "core.transform.project", Box::new(passthrough)));
        name_map.push((m!("Date Extractor"), "core.transform.project", Box::new(passthrough)));

        // ─── Script ───
        name_map.push((m!("Python Script"), "core.script.python", Box::new(python_config)));
        name_map.push((m!("Python View"), "core.script.python", Box::new(python_config)));
        name_map.push((m!("Java Snippet"), "core.script.wasm", Box::new(java_config)));
        name_map.push((m!("Java Snippet (simple)"), "core.script.wasm", Box::new(java_config)));
        name_map.push((m!("Java Edit Variable"), "core.script.wasm", Box::new(java_config)));

        // ─── SQL ───
        name_map.push((m!("SQL Node"), "core.sql", Box::new(sql_config)));

        // ─── Loop nodes → ForEach (approximation) ───
        name_map.push((contains!("Loop Start"), "core.transform.foreach", Box::new(passthrough)));
        name_map.push((contains!("Loop End"), "core.transform.foreach", Box::new(passthrough)));

        // ─── Variable / Config nodes → VariableCreator ───
        name_map.push((m!("Variable Creator"), "core.flow.variable_creator", Box::new(passthrough)));
        name_map.push((m!("Table Row to Variable"), "core.flow.table_to_variable", Box::new(passthrough)));
        name_map.push((m!("Variable to Table Row"), "core.flow.table_to_variable", Box::new(passthrough)));
        name_map.push((m!("Merge Variables"), "core.flow.variable_creator", Box::new(passthrough)));
        name_map.push((m!("Variable Expressions"), "core.flow.variable_creator", Box::new(passthrough)));
        name_map.push((m!("Table Column to Variable"), "core.flow.table_to_variable", Box::new(passthrough)));
        name_map.push((m!("Integer Configuration"), "core.flow.variable_creator", Box::new(passthrough)));
        name_map.push((m!("String Configuration"), "core.flow.variable_creator", Box::new(passthrough)));

        // ─── Config/Widget nodes → pass-through ───
        for &s in &[
            "Value Selection Configuration",
            "Column Filter Configuration",
            "Column Selection Configuration",
            "Date/Time Configuration",
            "Local File Browser Configuration",
            "Single Selection Configuration",
            "Multiple Selection Widget",
            "Nominal Row Filter Widget",
            "Repository File Chooser Configuration",
            "Breakpoint",
            "Timer Info",
        ] {
            name_map.push((m!(s), "core.transform.project", Box::new(passthrough)));
        }

        // ─── Visualization → Shaper SQL annotations ───
        for &(name, chart) in &[
            ("Bar Chart", "BARCHART"), ("Line Plot", "LINE"), ("Pie Chart", "PIE"),
            ("Scatter Plot", "SCATTER"), ("Scatter Plot Matrix", "SCATTER"),
            ("Histogram", "HISTOGRAM"), ("Heatmap", "HEATMAP"),
            ("Box Plot", "BOXPLOT"), ("Stacked Area Chart", "AREA"),
            ("Sunburst Chart", "SUNBURST"), ("Density Plot", "DENSITY"),
            ("Table View", "TABLE"), ("Generic ECharts View", "ECHART"),
            ("Parallel Coordinates Plot", "PARALLEL"), ("Bubble Chart", "BUBBLE"),
            ("Radar Plot", "RADAR"), ("Violin Plot", "VIOLIN"),
            ("Contour Plot", "CONTOUR"), ("2D Density Plot", "DENSITY"),
            ("Error Bars Plot", "ERRORBAR"), ("Generic JavaScript View", "ECHART"),
            ("Tile View", "TABLE"), ("Table Editor", "TABLE"),
            ("Tag Cloud", "TAGCLOUD"), ("Radar Plot Appender", "RADAR"),
            ("Spark Line Appender", "SPARKLINE"),
        ] {
            name_map.push((m!(name), "core.sql", Box::new(shaper_viz(chart))));
        }

        // ─── Report nodes → Shaper ───
        name_map.push((m!("Report PDF Writer"), "core.sql", Box::new(shaper_viz("REPORT"))));
        name_map.push((m!("Report HTML Writer"), "core.sql", Box::new(shaper_viz("REPORT"))));
        name_map.push((m!("Report Template Creator"), "core.sql", Box::new(shaper_viz("REPORT"))));

        // ─── Image nodes → pass-through ───
        for &s in &[
            "Image to Report", "Image to Table", "Image View",
            "Image Writer (Port)", "Image Writer (Table)",
            "Renderer to Image", "Table to Image", "Read Images",
        ] {
            name_map.push((m!(s), "core.transform.project", Box::new(passthrough)));
        }

        // ─── ML / PMML ───
        for &s in &[
            "PMML Reader", "PMML Writer", "Decision Tree Learner",
            "Decision Tree Predictor", "Linear Regression Learner",
            "Scorer", "Partitioning", "Auto-Binner",
            "Numeric Outliers", "SMOTE",
        ] {
            name_map.push((m!(s), "core.transform.project", Box::new(passthrough)));
        }

        // ─── Component I/O ───
        name_map.push((m!("Component Input"), "core.flow.variable_creator", Box::new(passthrough)));
        name_map.push((m!("Component Output"), "core.flow.table_to_variable", Box::new(passthrough)));

        // ─── Email / Network / SharePoint ───
        name_map.push((m!("Send Email"), "core.io.http_fetch", Box::new(passthrough)));
        for &s in &[
            "Microsoft Authenticator", "SharePoint Online Connector",
            "Mountpoint Connector", "Conda Environment Propagation",
        ] {
            name_map.push((m!(s), "core.io.http_fetch", Box::new(passthrough)));
        }

        Self { name_map }
    }

    /// Convert a KnwfArchive into a SwanFlow Workflow.
    pub async fn convert(&self, archive: KnwfArchive) -> KnimeResult<Workflow> {
        let mut graph =
            petgraph::stable_graph::StableGraph::<NodeInstance, EdgeInstance>::new();
        let mut id_to_idx: HashMap<String, NodeIndex> = HashMap::new();

        // Phase 1: Add all top-level nodes
        for knime_node in &archive.nodes {
            let (swanflow_type, config) = self.resolve_node(knime_node.name.as_deref(), &knime_node.settings);

            let instance = NodeInstance {
                id: Uuid::new_v4(),
                type_id: swanflow_type,
                config,
                state: NodeState::Unconfigured,
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

        // Phase 2: Add edges
        for edge in &archive.edges {
            if let (Some(&src), Some(&tgt)) =
                (id_to_idx.get(&edge.source_id), id_to_idx.get(&edge.target_id))
            {
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

        Ok(Workflow {
            id: Uuid::new_v4(),
            name: "Imported KNIME Workflow".to_string(),
            context: HashMap::new(),
            graph,
        })
    }

    /// Resolve a KNIME node to (swanflow_type_id, config_json).
    pub fn resolve_node(&self, name: Option<&str>, settings: &Value) -> (String, Value) {
        let raw_name = name.unwrap_or("Unknown");
        // Strip trailing " (#N)" from KNIME node display names
        let name: &str = raw_name.split(" (#").next().unwrap_or(raw_name).trim();

        // Special case: metanodes with no meaningful name
        if name.len() <= 5 && name.chars().all(|c| c == '_' || c == 'n') {
            return ("core.transform.project".into(), json!({"pass_through": true}));
        }

        for (matcher, sw_type, extractor) in &self.name_map {
            if matcher.matches(name) {
                return (sw_type.to_string(), extractor(settings));
            }
        }

        // Fallback: unknown node → passthrough sentinel
        ("core.util.unknown".into(), json!({
            "pass_through": true,
            "knime_name": name,
        }))
    }
}

impl Default for KnimeNodeMapper {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Config extractors ───

fn csv_read_config(s: &Value) -> Value {
    json!({
        "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
        "has_header": s.get("has_column_header").and_then(|v| v.as_bool()).unwrap_or(true),
        "delimiter": s.get("column_delimiter").and_then(|v| v.as_str()).unwrap_or(",")
    })
}

fn csv_write_config(s: &Value) -> Value {
    json!({
        "path": s.get("output_file").and_then(|v| v.as_str()).unwrap_or(""),
        "delimiter": s.get("column_delimiter").and_then(|v| v.as_str()).unwrap_or(",")
    })
}

fn excel_read_config(s: &Value) -> Value {
    json!({
        "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or(""),
        "sheet_name": s.get("sheet_name").and_then(|v| v.as_str())
    })
}

fn excel_write_config(s: &Value) -> Value {
    json!({
        "path": s.get("output_file").and_then(|v| v.as_str()).unwrap_or(""),
        "sheet_name": s.get("sheet_name").and_then(|v| v.as_str()).unwrap_or("Sheet1")
    })
}

fn table_read_config(s: &Value) -> Value {
    json!({ "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("") })
}

fn table_write_config(s: &Value) -> Value {
    json!({
        "table": s.get("table_name").and_then(|v| v.as_str()).unwrap_or("output"),
        "path": s.get("output_file").and_then(|v| v.as_str()).unwrap_or("")
    })
}

fn file_read_config(s: &Value) -> Value {
    json!({ "path": s.get("file_name").and_then(|v| v.as_str()).unwrap_or("") })
}

fn db_read_config(s: &Value) -> Value {
    json!({
        "query": s.get("sql_string").and_then(|v| v.as_str()).unwrap_or(""),
        "connection": s.get("database_url").and_then(|v| v.as_str()).unwrap_or("")
    })
}

fn filter_config(s: &Value) -> Value {
    let col = s.get("column_name").and_then(|v| v.as_str()).unwrap_or("");
    let op = map_op(s.get("operator").and_then(|v| v.as_str()).unwrap_or("="));
    let val = s.get("filter_value").and_then(|v| v.as_str()).unwrap_or("");
    json!({"predicate": format!("{} {} {}", col, op, val)})
}

fn column_filter_config(s: &Value) -> Value {
    json!({"columns": s.get("included_column_names").and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())})
}

fn column_rename_config(s: &Value) -> Value {
    json!({"renames": s.get("column_renames").cloned().unwrap_or(json!({}))})
}

fn groupby_config(s: &Value) -> Value {
    json!({
        "group_by": s.get("group_column_names").and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
        "aggregates": s.get("aggregate_column_names").and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
    })
}

fn pivot_config(s: &Value) -> Value {
    json!({
        "group_by": s.get("group_column_names").and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
        "pivot_column": s.get("pivot_column").and_then(|v| v.as_str())
    })
}

fn joiner_config(s: &Value) -> Value {
    json!({
        "left_keys": s.get("left_column_names").and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
        "right_keys": s.get("right_column_names").and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>()),
        "join_type": s.get("join_type").and_then(|v| v.as_str()).unwrap_or("INNER"),
    })
}

fn rule_engine_config(s: &Value) -> Value {
    json!({"rules": s.get("rules").cloned().unwrap_or(json!([]))})
}

fn math_formula_config(s: &Value) -> Value {
    json!({"expressions": [{
        "column": s.get("new_column_name").and_then(|v| v.as_str()).unwrap_or("result"),
        "formula": s.get("expression").and_then(|v| v.as_str()).unwrap_or("")
    }]})
}

fn constant_column_config(s: &Value) -> Value {
    json!({
        "expressions": [{
            "column": s.get("new_column_name").and_then(|v| v.as_str()).unwrap_or("constant"),
            "formula": s.get("constant_value").and_then(|v| v.as_str()).unwrap_or("")
        }]
    })
}

fn sorter_config(s: &Value) -> Value {
    json!({
        "sort_key": s.get("column_name").and_then(|v| v.as_str()).unwrap_or(""),
        "ascending": !s.get("sort_descending").and_then(|v| v.as_bool()).unwrap_or(false)
    })
}

fn python_config(s: &Value) -> Value {
    json!({"script": s.get("python_script").and_then(|v| v.as_str()).unwrap_or("")})
}

fn java_config(s: &Value) -> Value {
    json!({
        "wasm_fallback": true,
        "script": s.get("java_snippet").and_then(|v| v.as_str()).unwrap_or(""),
        "knime_name": "Java Snippet"
    })
}

fn sql_config(s: &Value) -> Value {
    json!({"query": s.get("sql_string").and_then(|v| v.as_str()).unwrap_or("")})
}

fn table_creator_config(s: &Value) -> Value {
    json!({
        "inline_data": s.get("data").cloned().unwrap_or(json!([])),
        "columns": s.get("columns").cloned().unwrap_or(json!([]))
    })
}

fn shaper_viz(chart_type: &'static str) -> Box<dyn Fn(&Value) -> Value + Send + Sync> {
    Box::new(move |_s: &Value| {
        json!({
            "shaper": {
                "chart_type": chart_type,
                "annotations": true
            }
        })
    })
}

fn passthrough(_s: &Value) -> Value {
    json!({"pass_through": true})
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

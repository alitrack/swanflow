use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore, Extension};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::nodes::{NodeBehavior, TypedNodeBehavior, NodeSpec, PortSpec};
use crate::core::datasource::{DuckDBScanExec, DuckDBScanLogical};

/// LuaJIT shell node — runs a Lua function through the duckdb-luajit
/// extension. This is the "self-evolution" escape hatch: a model writes a
/// Lua function (either standalone table generator or scalar transform),
/// and this node makes it appear on the canvas as a first-class node.
///
/// Two modes:
/// - `table`: `SELECT * FROM luajit_table(source)` — no input port, acts as
///   a data generator/reader (dirscan, dicom, id3, zip_list, ...).
/// - `scalar`: `SELECT * FROM (SELECT luajit_s(source, col) AS result FROM input_0)`
///   — transforms the upstream input table with a scalar UDF.
/// - `action`: `SELECT luajit_s(source, {op:'run', ...}) AS result` — executes an
///   action lib (incremental/scd2: DML/DDL against DuckDB tables) with a JSON
///   opts struct from config `args`; returns a single-row JSON string.
pub struct LuaJitNode;

#[async_trait]
impl NodeBehavior for LuaJitNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.luajit".to_string(),
            display_name: "LuaJIT Script".to_string(),
            description: "Execute a LuaJIT UDF via the duckdb-luajit extension (table generator or scalar transform)"
                .to_string(),
            // Table mode has no inputs; scalar mode uses input_0. Ports are
            // declared here (planner tolerates unused inputs), TypedNodeBehavior
            // reports the fixed arity used by the executor.
            input_ports: vec![PortSpec {
                id: 0,
                name: "input_0".into(),
                description: "Input table (scalar mode only)".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["table", "scalar", "action"],
                        "default": "table"
                    },
                    "source": { "type": "string", "description": "Lua source passed to luajit_table/luajit_s" },
                    "sql_name": {
                        "type": "string",
                        "description": "Optional registered UDF name (luajit_module install/quick_compile)"
                    },
                    "args": {
                        "type": ["object", "array"],
                        "description": "action mode: opts struct passed to luajit_s, e.g. {\"op\":\"run\",\"target\":\"tgt\",\"source\":\"src\",\"ts_col\":\"ts\",\"mode\":\"ts\"}"
                    },
                    "columns": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Columns to apply scalar mode to (default: all)"
                    }
                },
                "required": ["source"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let source = config["source"]
            .as_str()
            .ok_or_else(|| anyhow!("Lua source is required"))?;
        if source.trim().is_empty() {
            return Err(anyhow!("Lua source must not be empty"));
        }
        let mode = config["mode"].as_str().unwrap_or("table");
        if mode != "table" && mode != "scalar" && mode != "action" {
            return Err(anyhow!("mode must be 'table', 'scalar' or 'action', got '{mode}'"));
        }
        if mode == "action" && config.get("args").is_none() {
            return Err(anyhow!("action mode requires an 'args' object"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let source = config["source"].as_str().unwrap();
        let mode = config["mode"].as_str().unwrap_or("table");

        // Single-quote escaping for embedding Lua source into SQL.
        let esc = source.replace('\'', "''");

        let query = match mode {
            "table" => {
                // luajit_table('lua source') — table generator, no inputs.
                format!("SELECT * FROM luajit_table('{}')", esc)
            }
            "scalar" => {
                let input = inputs
                    .first()
                    .ok_or_else(|| anyhow!("scalar mode requires an input table"))?;
                // Determine target columns: config.columns or all columns of input.
                let cols: Vec<String> = if let Some(c) = config["columns"].as_array() {
                    c.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                } else {
                    input
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect()
                };
                if cols.is_empty() {
                    return Err(anyhow!("scalar mode needs at least one column to apply to"));
                }
                // Wrap the upstream plan as input_0 and apply the scalar UDF to
                // each selected column, producing a result column per input.
                let selects: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        // luajit_s(source, col) — value flows as second arg.
                        // Quote-safe column refs.
                        format!("luajit_s('{}', \"{}\") AS \"{}\"", esc, c, c)
                    })
                    .collect();
                // Preserve the full input row plus computed columns. This
                // keeps the node a pure transform (output schema = input + results).
                let all_input_cols: Vec<String> = input
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| format!("\"{}\"", f.name()))
                    .collect();
                let select_all = if all_input_cols.is_empty() {
                    selects.join(", ")
                } else {
                    let mut parts = all_input_cols;
                    parts.extend(selects);
                    parts.join(", ")
                };
                format!(
                    "SELECT * FROM (SELECT {select_all} FROM input_0) AS _luajit_transform"
                )
            }
            "action" => {
                // Module-table libs: install registers the lib (cached
                // source), then luajit_s resolves the table's .run method
                // (luajit extension v0.32+). CTE form keeps it one statement.
                let args = config.get("args").cloned().unwrap_or(JsonValue::Null);
                let args_sql = struct_literal(&args)?;
                format!(
                    "WITH _reg AS (SELECT * FROM luajit_module(mode:='install', sql_name:='{}')) SELECT luajit_s('{}', {}) AS result FROM _reg",
                    esc, esc, args_sql
                )
            }
            _ => unreachable!("validated above"),
        };

        // Early schema discovery: run the query against a scratch DuckDB
        // connection with the luajit extension loaded, so the logical schema
        // is accurate (planner needs it before physical execution).
        let schema: DFSchemaRef = discover_schema(&query, config).unwrap_or_else(|_| {
            if !inputs.is_empty() {
                inputs[0].schema().clone()
            } else {
                Arc::new(DFSchema::empty())
            }
        });

        let node = DuckDBScanLogical {
            connection_path: ":memory:".to_string(),
            extensions: luajit_extension_paths(),
            attach_sql: None,
            query,
            schema,
        };

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })])
    }
}

impl TypedNodeBehavior for LuaJitNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

/// Render a JSON value as a DuckDB literal (struct/list/string/number/bool).
/// DuckDB struct literal syntax (verified on 1.5.5): `{'op':'run','n':1}` —
/// keys single-quoted, strings single-quoted and escaped, numbers/bools bare,
/// null → NULL. Nested objects/lists render recursively.
fn struct_literal(v: &JsonValue) -> Result<String> {
    match v {
        JsonValue::Object(o) => {
            let parts: Result<Vec<String>> = o
                .iter()
                .map(|(k, val)| Ok(format!("'{}':{}", k, struct_literal(val)?)))
                .collect();
            Ok(format!("{{{}}}", parts?.join(", ")))
        }
        JsonValue::Array(a) => {
            let parts: Result<Vec<String>> = a.iter().map(struct_literal).collect();
            Ok(format!("[{}]", parts?.join(", ")))
        }
        JsonValue::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        JsonValue::Number(n) => Ok(n.to_string()),
        JsonValue::Bool(b) => Ok(if *b { "true".to_string() } else { "false".to_string() }),
        JsonValue::Null => Ok("NULL".to_string()),
    }
}

/// A LuaJitNode variant pre-bound to a specific duckdb-luajit-libs library
/// (e.g. `luajit.dirscan`). Auto-registered by scanning the libs INDEX —
/// this is how "new nodes grow on the canvas" without writing Rust code.
pub struct LuaJitLibNode {
    pub lib_name: String,
    pub source: String,
    pub description: String,
    /// Action-mode libs (incremental/scd2): default args template pre-filled
    /// into the node config. None = table/scalar lib.
    pub action_args: Option<JsonValue>,
}

#[async_trait]
impl NodeBehavior for LuaJitLibNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: format!("luajit.{}", self.lib_name),
            display_name: format!("LuaJIT {}", self.lib_name),
            description: format!(
                "Auto-registered from duckdb-luajit-libs: {}",
                self.description
            ),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input_0".into(),
                description: "Input table (scalar mode only)".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string", "enum": ["table", "scalar", "action"], "default": "table" },
                    "args": {
                        "type": ["object", "array"],
                        "description": "action mode opts (e.g. {\"op\":\"run\",\"target\":\"tgt\",\"source\":\"src\",\"ts_col\":\"ts\",\"mode\":\"ts\"}); table/scalar libs ignore this"
                    },
                    "backend": {
                        "type": "string",
                        "enum": ["auto", "gpu", "cpu"],
                        "default": "auto",
                        "description": "Compute backend for JSON-args libs (linalg): auto = env-gated GPU (LUA_LINALG_GPU=1), gpu = force GPU, cpu = force CPU. Merged into args as 'backend'."
                    }
                }
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        // Delegate to the generic shell node with the lib source pre-filled.
        let mut cfg = config.clone();
        cfg["source"] = serde_json::Value::String(self.source.clone());
        // Action libs: pre-fill mode=action + args template unless the user
        // explicitly configured them.
        if let Some(args) = &self.action_args {
            let mode = cfg.get("mode").and_then(|m| m.as_str()).unwrap_or("");
            if mode != "action" {
                cfg["mode"] = serde_json::Value::String("action".into());
            }
            if cfg.get("args").is_none() {
                cfg["args"] = args.clone();
            }
        }
        // Backend override for JSON-args libs (linalg): merge into args so
        // the Lua side sees `backend: "gpu"|"cpu"` per call. 'auto' (default)
        // leaves args untouched (env-gated GPU selection in linalg.lua).
        let backend_override = cfg
            .get("backend")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_default();
        if !backend_override.is_empty() && backend_override != "auto" {
            if let Some(args) = cfg.get_mut("args") {
                if args.is_object() {
                    args["backend"] = serde_json::Value::String(backend_override);
                }
            }
        }
        let inner = LuaJitNode;
        inner.propose_plan(inputs, &cfg).await
    }
}

impl TypedNodeBehavior for LuaJitLibNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

/// Scan a duckdb-luajit-libs INDEX file and produce pre-bound node variants.
///
/// INDEX format: one `name|path` per line. The `source` for each lib is the
/// lib's registered name (luajit extension resolves installed libs by name
/// through its INDEX cache: `luajit_table('dirscan')` etc.), so we don't need
/// to inline the full Lua source.
pub fn luajit_libs_from_index(index_path: &str) -> Vec<LuaJitLibNode> {
    let content = match std::fs::read_to_string(index_path) {
        Ok(c) => c,
        Err(_) => return vec![],
    };
    let mut nodes = Vec::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, _path) = match line.split_once('|') {
            Some(pair) => pair,
            None => continue,
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        // Action-mode libs (run DML/DDL via _duckdb_call, return JSON): give
        // their canvas node a pre-filled args template. New action libs should
        // be added here (kept in sync with duckdb-luajit-libs INDEX).
        let action_args = match name {
            "incremental" => Some(serde_json::json!({
                "op": "run", "target": "", "source": "", "ts_col": "", "mode": "ts"
            })),
            "scd2" => Some(serde_json::json!({
                "op": "run", "target": "", "source": "", "key_col": "",
                "attr_cols": [], "ts_col": ""
            })),
            // linalg: JSON-args matrix lib (GPU-capable). Prefill a 2×2 SVD
            // anchor case; user edits args to their own matrix/op.
            "linalg" => Some(serde_json::json!({
                "op": "svd", "a": [4, 7, 2, 6], "m": 2, "n": 2
            })),
            _ => None,
        };
        let description = if name == "linalg" {
            format!(
                "LuaJIT linalg lib (GPU-capable matmul/SVD/eigh/inv/lu/chol/qr — set args; backend: auto/gpu/cpu)"
            )
        } else {
            match action_args {
                Some(_) => format!(
                    "LuaJIT action lib '{}' (runs DML/DDL, returns JSON — fill args)",
                    name
                ),
                None => format!("LuaJIT lib '{}' from duckdb-luajit-libs", name),
            }
        };
        nodes.push(LuaJitLibNode {
            lib_name: name.to_string(),
            // libs are invoked by registered name; the extension's install
            // mechanism (luajit_module install / offline cache) resolves it.
            source: name.to_string(),
            description,
            action_args,
        });
    }
    nodes
}

/// Default locations checked for a duckdb-luajit-libs INDEX, in priority order:
/// 1. `SWANFLOW_LUAJIT_LIBS_INDEX` env var (explicit)
/// 2. local dev clone (used in this repo's dev environment)
/// 3. extension's offline cache dir
pub fn default_luajit_libs_index() -> Option<String> {
    if let Ok(p) = std::env::var("SWANFLOW_LUAJIT_LIBS_INDEX") {
        if !p.is_empty() && std::path::Path::new(&p).exists() {
            return Some(p);
        }
    }
    const CANDIDATES: &[&str] = &[
        "/mnt/d/wsl2/duckdb-luajit-libs/INDEX",
        "/home/lhy/.duckdb/luajit-libs/INDEX",
    ];
    CANDIDATES
        .iter()
        .find(|p| std::path::Path::new(p).exists())
        .map(|p| p.to_string())
}

/// Resolve the luajit extension load path.
///
/// Priority:
/// 1. `SWANFLOW_LUAJIT_EXT` env var (explicit, e.g. CI or local build path)
/// 2. `SWANFLOW_DUCKDB_EXTENSIONS` env var entries ending in luajit*
/// 3. default local build path used by the duckdb-luajit dev repo
///
/// Falls back to bare `luajit` (registry install) when nothing is configured.
pub fn luajit_extension_paths() -> Vec<String> {
    if let Ok(p) = std::env::var("SWANFLOW_LUAJIT_EXT") {
        if !p.is_empty() {
            return vec![p];
        }
    }
    if let Ok(exts) = std::env::var("SWANFLOW_DUCKDB_EXTENSIONS") {
        let hit: Vec<String> = exts
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty() && s.contains("luajit"))
            .map(|s| s.to_string())
            .collect();
        if !hit.is_empty() {
            return hit;
        }
    }
    vec![
        "/mnt/d/wsl2/luajit/build/release/luajit.duckdb_extension".to_string(),
        "luajit".to_string(),
    ]
}

/// Try to determine the output schema of a luajit query by executing it
/// against a scratch DuckDB connection (unsigned extension allowed).
fn discover_schema(query: &str, _config: &JsonValue) -> Result<DFSchemaRef> {
    let config = duckdb::Config::default()
        .allow_unsigned_extensions()
        .map_err(|e| anyhow!(e.to_string()))?;
    let conn = duckdb::Connection::open_with_flags(":memory:", config)?;
    for ext in luajit_extension_paths() {
        let sql = if ext.contains('/') || ext.ends_with(".duckdb_extension") {
            format!("LOAD '{}'", ext)
        } else {
            format!("LOAD {}", ext)
        };
        let _ = conn.execute_batch(&sql);
    }
    // Prepare + execute, then read the duckdb crate's Arrow schema.
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query([])?;
    let duck_schema = rows
        .as_ref()
        .expect("statement reference after query")
        .schema();
    let fields: Vec<datafusion::arrow::datatypes::Field> = duck_schema
        .fields()
        .iter()
        .map(|f| {
            let dt = map_duckdb_type(&format!("{:?}", f.data_type()));
            datafusion::arrow::datatypes::Field::new(f.name().clone(), dt, true)
        })
        .collect();
    Ok(Arc::new(DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(fields))?))
}

fn map_duckdb_type(col_type: &str) -> datafusion::arrow::datatypes::DataType {
    use datafusion::arrow::datatypes::DataType;
    match col_type.to_uppercase().as_str() {
        "BOOLEAN" => DataType::Boolean,
        "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" | "INT8" | "INT16" | "INT32"
        | "INT64" | "UINT8" | "UINT16" | "UINT32" | "UINT64" => DataType::Int64,
        "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "FLOAT32" | "FLOAT64" => DataType::Float64,
        "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "DATETIME" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "DATE" => DataType::Date32,
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_libs_index_parsing() {
        // Write a small INDEX to a temp file.
        let tmp = std::env::temp_dir().join("luajit_index_test.txt");
        std::fs::write(
            &tmp,
            "dicom|libs/datasource/dicom.lua\n# comment\n\ndirscan|libs/datasource/dirscan.lua\nbadline\n",
        )
        .unwrap();
        let nodes = luajit_libs_from_index(tmp.to_str().unwrap());
        std::fs::remove_file(&tmp).ok();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].lib_name, "dicom");
        assert_eq!(nodes[1].lib_name, "dirscan");
        // spec type_ids
        assert_eq!(nodes[0].spec().type_id, "luajit.dicom");
        assert_eq!(nodes[1].spec().type_id, "luajit.dirscan");
        // non-action libs have no args template
        assert!(nodes[0].action_args.is_none());
    }

    #[test]
    fn test_libs_index_action_libs() {
        let tmp = std::env::temp_dir().join("luajit_index_action_test.txt");
        std::fs::write(
            &tmp,
            "incremental|libs/etl/incremental.lua\nscd2|libs/etl/scd2.lua\nlinalg|libs/linalg/linalg.lua\nbase64|libs/udf/base64.lua\n",
        )
        .unwrap();
        let nodes = luajit_libs_from_index(tmp.to_str().unwrap());
        std::fs::remove_file(&tmp).ok();
        assert_eq!(nodes.len(), 4);
        let incr = nodes.iter().find(|n| n.lib_name == "incremental").unwrap();
        assert!(incr.action_args.is_some());
        assert_eq!(incr.action_args.as_ref().unwrap()["op"], "run");
        let scd2 = nodes.iter().find(|n| n.lib_name == "scd2").unwrap();
        assert!(scd2.action_args.is_some());
        // linalg: JSON-args GPU-capable lib → action template with SVD anchor
        let linalg = nodes.iter().find(|n| n.lib_name == "linalg").unwrap();
        assert!(linalg.action_args.is_some());
        assert_eq!(linalg.action_args.as_ref().unwrap()["op"], "svd");
        assert!(linalg.description.contains("GPU-capable"));
        let b64 = nodes.iter().find(|n| n.lib_name == "base64").unwrap();
        assert!(b64.action_args.is_none());
    }

    #[test]
    fn test_struct_literal() {
        assert_eq!(
            struct_literal(&serde_json::json!({"op": "run", "n": 1, "b": true, "x": null})).unwrap(),
            "{'op':'run', 'n':1, 'b':true, 'x':NULL}"
        );
        // single-quote escaping + nested list
        assert_eq!(
            struct_literal(&serde_json::json!({"attr_cols": ["city", "tier"], "q": "it's"})).unwrap(),
            "{'attr_cols':['city', 'tier'], 'q':'it''s'}"
        );
    }

    #[test]
    fn test_libs_index_missing_file() {
        assert!(luajit_libs_from_index("/nonexistent/INDEX").is_empty());
    }

    /// Real-world check: register_core_nodes auto-registers luajit libs from
    /// the dev INDEX (when present). Verifies the self-evolution wiring.
    #[test]
    fn test_register_core_nodes_has_luajit_libs() {
        let mut registry = crate::nodes::registry::NodeRegistry::new();
        registry.register_core_nodes();
        let type_ids = registry.list_specs();
        assert!(
            type_ids.iter().any(|s| s.type_id == "core.luajit"),
            "core.luajit should be registered"
        );
        let lib_nodes: Vec<_> = type_ids
            .iter()
            .filter(|s| s.type_id.starts_with("luajit."))
            .collect();
        if let Some(index) = default_luajit_libs_index() {
            let expected = luajit_libs_from_index(&index).len();
            assert!(
                lib_nodes.len() == expected,
                "expected {expected} luajit lib nodes, got {}",
                lib_nodes.len()
            );
            println!(
                "[luajit libs] {} auto-registered: {}",
                lib_nodes.len(),
                lib_nodes
                    .iter()
                    .map(|s| s.type_id.clone())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        } else {
            assert!(
                lib_nodes.is_empty(),
                "no INDEX should mean no lib nodes, got {}",
                lib_nodes.len()
            );
        }
    }

    #[test]
    fn test_spec_and_validate() {
        let node = LuaJitNode;
        let spec = node.spec();
        assert_eq!(spec.type_id, "core.luajit");

        // valid table mode
        node.validate(&serde_json::json!({"source": "return {\"a\"}"})).unwrap();
        // valid scalar mode
        node.validate(&serde_json::json!({"mode": "scalar", "source": "return x"})).unwrap();
        // valid action mode (with args)
        node.validate(&serde_json::json!({"mode": "action", "source": "x", "args": {"op": "run"}})).unwrap();
        // action mode without args → error
        assert!(node.validate(&serde_json::json!({"mode": "action", "source": "x"})).is_err());
        // missing source
        assert!(node.validate(&serde_json::json!({})).is_err());
        // bad mode
        assert!(node.validate(&serde_json::json!({"mode": "nope", "source": "x"})).is_err());
    }

    #[test]
    fn test_query_building_table_mode() {
        let node = LuaJitNode;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let plans = node
                .propose_plan(
                    vec![],
                    &serde_json::json!({"source": "return {\"a\", \"b\"}"}),
                )
                .await
                .unwrap();
            assert_eq!(plans.len(), 1);
            if let LogicalPlan::Extension(ext) = &plans[0] {
                let scan = ext
                    .node
                    .as_any()
                    .downcast_ref::<DuckDBScanLogical>()
                    .expect("should be DuckDBScanLogical");
                assert!(scan.query.contains("luajit_table"));
                assert!(scan.query.contains("return"));
                assert!(!scan.extensions.is_empty());
            } else {
                panic!("expected Extension plan");
            }
        });
    }

    #[test]
    fn test_query_building_action_mode() {
        let node = LuaJitNode;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let plans = node
                .propose_plan(
                    vec![],
                    &serde_json::json!({
                        "mode": "action",
                        "source": "incremental",
                        "args": {"op": "run", "target": "tgt", "source": "src", "ts_col": "ts", "mode": "ts"}
                    }),
                )
                .await
                .unwrap();
            assert_eq!(plans.len(), 1);
            if let LogicalPlan::Extension(ext) = &plans[0] {
                let scan = ext
                    .node
                    .as_any()
                    .downcast_ref::<DuckDBScanLogical>()
                    .expect("should be DuckDBScanLogical");
                assert!(
                    scan.query.contains("WITH _reg AS (SELECT * FROM luajit_module(mode:='install', sql_name:='incremental')) SELECT luajit_s('incremental', {'op':'run', 'target':'tgt', 'source':'src', 'ts_col':'ts', 'mode':'ts'}) AS result FROM _reg"),
                    "unexpected action SQL: {}",
                    scan.query
                );
            } else {
                panic!("expected Extension plan");
            }
        });
    }

    /// Action lib pre-fill: LuaJitLibNode with action_args auto-configures
    /// mode=action + args template on an empty config.
    #[test]
    fn test_lib_node_action_prefill() {
        let node = LuaJitLibNode {
            lib_name: "incremental".to_string(),
            source: "incremental".to_string(),
            description: "test".to_string(),
            action_args: Some(serde_json::json!({
                "op": "run", "target": "", "source": "", "ts_col": "", "mode": "ts"
            })),
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let plans = node
                .propose_plan(vec![], &serde_json::json!({}))
                .await
                .unwrap();
            if let LogicalPlan::Extension(ext) = &plans[0] {
                let scan = ext
                    .node
                    .as_any()
                    .downcast_ref::<DuckDBScanLogical>()
                    .expect("should be DuckDBScanLogical");
                assert!(
                    scan.query.contains("WITH _reg AS (SELECT * FROM luajit_module(mode:='install', sql_name:='incremental')) SELECT luajit_s('incremental', {'op':'run', 'target':'', 'source':'', 'ts_col':'', 'mode':'ts'}) AS result FROM _reg"),
                    "unexpected prefilled action SQL: {}",
                    scan.query
                );
            } else {
                panic!("expected Extension plan");
            }
        });
    }

    /// Backend override: LuaJitLibNode config `backend: gpu|cpu` merges into
    /// the args JSON so the Lua lib (linalg) selects the compute backend.
    #[test]
    fn test_lib_node_backend_merge() {
        let node = LuaJitLibNode {
            lib_name: "linalg".to_string(),
            source: "linalg".to_string(),
            description: "test".to_string(),
            action_args: Some(serde_json::json!({
                "op": "svd", "a": [4, 7, 2, 6], "m": 2, "n": 2
            })),
        };
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // backend=gpu → args carry 'backend':'gpu'
            let plans = node
                .propose_plan(vec![], &serde_json::json!({"backend": "gpu"}))
                .await
                .unwrap();
            if let LogicalPlan::Extension(ext) = &plans[0] {
                let scan = ext
                    .node
                    .as_any()
                    .downcast_ref::<DuckDBScanLogical>()
                    .expect("should be DuckDBScanLogical");
                assert!(
                    scan.query.contains("'backend':'gpu'"),
                    "backend=gpu should merge into args: {}",
                    scan.query
                );
                assert!(scan.query.contains("luajit_s('linalg'"), "linalg action SQL: {}", scan.query);
            } else {
                panic!("expected Extension plan");
            }
            // backend=cpu → args carry 'backend':'cpu'
            let plans2 = node
                .propose_plan(vec![], &serde_json::json!({"backend": "cpu"}))
                .await
                .unwrap();
            if let LogicalPlan::Extension(ext) = &plans2[0] {
                let scan = ext
                    .node
                    .as_any()
                    .downcast_ref::<DuckDBScanLogical>()
                    .expect("should be DuckDBScanLogical");
                assert!(
                    scan.query.contains("'backend':'cpu'"),
                    "backend=cpu should merge into args: {}",
                    scan.query
                );
            } else {
                panic!("expected Extension plan");
            }
            // backend=auto (default) → args untouched
            let plans3 = node
                .propose_plan(vec![], &serde_json::json!({}))
                .await
                .unwrap();
            if let LogicalPlan::Extension(ext) = &plans3[0] {
                let scan = ext
                    .node
                    .as_any()
                    .downcast_ref::<DuckDBScanLogical>()
                    .expect("should be DuckDBScanLogical");
                assert!(
                    !scan.query.contains("'backend'"),
                    "auto backend should leave args untouched: {}",
                    scan.query
                );
            } else {
                panic!("expected Extension plan");
            }
        });
    }

    /// End-to-end: shell node plan → DuckDBScanExec physical execution →
    /// RecordBatch rows. Requires SWANFLOW_LUAJIT_EXT pointing at a local
    /// luajit.duckdb_extension.
    #[test]
    fn test_luajit_node_end_to_end() {
        let ext = match std::env::var("SWANFLOW_LUAJIT_EXT") {
            Ok(p) if !p.is_empty() => p,
            _ => return, // not configured — skip
        };
        std::env::set_var("SWANFLOW_LUAJIT_EXT", &ext);
        let node = LuaJitNode;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Build the logical plan from the shell node.
            let plans = node
                .propose_plan(vec![], &serde_json::json!({"source": "return {\"a\", \"b\"}"}))
                .await
                .unwrap();
            let LogicalPlan::Extension(ext_plan) = &plans[0] else {
                panic!("expected Extension plan");
            };
            let scan = ext_plan
                .node
                .as_any()
                .downcast_ref::<DuckDBScanLogical>()
                .expect("should be DuckDBScanLogical");

            // Convert to the physical exec the way SwanFlowPlanner does:
            // project_schema = logical schema fields.
            let schema: SchemaRef = Arc::new(
                datafusion::arrow::datatypes::Schema::new(
                    scan.schema
                        .fields()
                        .iter()
                        .map(|f| {
                            datafusion::arrow::datatypes::Field::new(
                                f.name().clone(),
                                f.data_type().clone(),
                                true,
                            )
                        })
                        .collect::<Vec<_>>(),
                ),
            );
            let exec = DuckDBScanExec::new(
                scan.connection_path.clone(),
                scan.extensions.clone(),
                scan.attach_sql.clone(),
                scan.query.clone(),
                schema,
            );

            // Execute via DataFusion task context.
            let ctx = datafusion::prelude::SessionContext::new();
            let state = ctx.state();
            use datafusion::physical_plan::ExecutionPlan as _;
            let stream = exec.execute(0, state.task_ctx()).unwrap();
            let batches: Vec<RecordBatch> = futures::executor::block_on(
                futures::StreamExt::collect::<Vec<_>>(stream),
            )
            .into_iter()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
            assert!(!batches.is_empty());
            let total = batches.iter().map(|b| b.num_rows()).sum::<usize>();
            assert_eq!(total, 2, "luajit_table should yield 2 rows");
            println!(
                "[luajit e2e] {} rows, schema: {:?}",
                total,
                batches[0].schema()
            );
        });
    }
}

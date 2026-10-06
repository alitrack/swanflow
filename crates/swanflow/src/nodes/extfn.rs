//! Auto-registered DuckDB extension-function nodes.
//!
//! This is the **second source** for the canvas "self-evolution" mechanism.
//! The first is [`crate::nodes::luajit`] (scans the `duckdb-luajit-libs/INDEX`
//! manifest, 47 libs). This one reads a *materialized* `ext_functions` table
//! built by `scripts/build_ext_functions.py` out of DuckDB's own
//! `duckdb_functions()` catalog, so any extension function can appear on the
//! canvas without writing Rust.
//!
//! Design record: `openspec/changes/extfn-auto-nodes/design.md`.
//!
//! Why the catalog is materialized rather than queried live:
//!   * `duckdb_functions()` only reports functions of extensions that are
//!     already LOADed, so a live scan would have to LOAD every extension at
//!     startup (slow, and it can fail);
//!   * dev/alpha DuckDB builds publish no extension binaries, so `INSTALL`
//!     fails and the loadable set is whatever is already on disk;
//!   * a table carries the human curation flag (`enabled`), which is the
//!     node-bloat guard.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use serde_json::Value as JsonValue;
use std::sync::Arc;

use crate::core::datasource::DuckDBScanLogical;
use crate::nodes::luajit::{discover_schema, struct_literal};
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

/// One row of the materialized `ext_functions` catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtFnDef {
    pub extension: String,
    pub function: String,
    pub function_type: String,
    pub return_type: Option<String>,
    pub description: Option<String>,
    pub has_side_effects: bool,
}

/// A canvas node bound to one DuckDB extension function, e.g.
/// `ext.spatial.st_area`.
///
/// The extension and function names are baked into `type_id` (not into config)
/// so that two extensions exporting the same function name cannot silently
/// overwrite each other — `NodeRegistry::register_node` is a `HashMap::insert`.
pub struct ExtFnNode {
    pub def: ExtFnDef,
}

impl ExtFnNode {
    /// `ext.<extension>.<function>` — collision-free across extensions.
    pub fn type_id_for(extension: &str, function: &str) -> String {
        format!("ext.{}.{}", extension, function)
    }

    /// `table` functions are generators/scanners (no real input).
    /// Everything else (`scalar`/`aggregate`) is treated as a column transform.
    pub fn is_generator(&self) -> bool {
        self.def.function_type == "table"
    }

    /// Build the SQL for this node.
    ///
    /// Split out of `propose_plan` so it can be unit-tested without a
    /// DataFusion runtime or an input `LogicalPlan`.
    ///
    /// * `table` mode: `SELECT * FROM <fn>(<args>)` — no input.
    /// * `scalar` mode: pass each column in `config.columns` as the function's
    ///   first argument, preserving the input row:
    ///   `SELECT * FROM (SELECT <input cols>, <fn>(col, args) AS col ... FROM input_0)`
    pub fn build_query(
        &self,
        input_cols: &[String],
        config: &JsonValue,
    ) -> Result<String> {
        let f = &self.def.function;
        let args_sql = args_to_sql(config.get("args"))?;
        let mode = mode_of(config, self.is_generator());

        if mode == "table" {
            return Ok(if args_sql.is_empty() {
                format!("SELECT * FROM {f}()")
            } else {
                format!("SELECT * FROM {f}({args_sql})")
            });
        }

        let cols: Vec<String> = config
            .get("columns")
            .and_then(|c| c.as_array())
            .map(|c| {
                c.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if cols.is_empty() {
            return Err(anyhow!(
                "scalar mode requires 'columns' (which input columns to pass to {f})"
            ));
        }

        let selects: Vec<String> = cols
            .iter()
            .map(|c| {
                let call = if args_sql.is_empty() {
                    format!("{f}({})", quote_ident(c))
                } else {
                    format!("{f}({}, {})", quote_ident(c), args_sql)
                };
                // Alias disambiguates from the passthrough column of the same
                // name (sigil review med-1: same-name output columns are
                // ambiguous for name-based downstream consumers).
                format!("{call} AS {}", quote_ident(&format!("{c}__extfn")))
            })
            .collect();

        let mut parts: Vec<String> = input_cols.iter().map(|c| quote_ident(c)).collect();
        parts.extend(selects);
        Ok(format!(
            "SELECT * FROM (SELECT {} FROM input_0) AS _extfn_transform",
            parts.join(", ")
        ))
    }
}

#[async_trait]
impl NodeBehavior for ExtFnNode {
    fn spec(&self) -> NodeSpec {
        let d = &self.def;

        // Be honest about provenance: signatures are auto-extracted, prose
        // usually is not. Measured on DuckDB v2.0: `parameter_types` is filled
        // for 98.1% of rows, but `description` is filled only for
        // `core_functions` (99.8%), `spatial` (95.8%) and `ai` (100%) — every
        // other inspected extension is at 0%. So never claim the generated node
        // is self-documenting.
        let mut desc = format!(
            "DuckDB extension function `{}.{}` ({}). Returns {}.",
            d.extension,
            d.function,
            d.function_type,
            d.return_type.as_deref().unwrap_or("(unknown)")
        );
        if d.has_side_effects {
            desc.push_str(" ⚠ has side effects (action-type operator).");
        }
        match d.description.as_deref().filter(|s| !s.is_empty()) {
            Some(human) => {
                desc.push(' ');
                desc.push_str(human);
            }
            None => desc.push_str(
                " No upstream description available — arguments are inferred from the signature only.",
            ),
        }

        NodeSpec {
            type_id: Self::type_id_for(&d.extension, &d.function),
            display_name: format!("{} ({})", d.function, d.extension),
            description: desc,
            // Mirrors `LuaJitLibNode`: input_0 is declared for transform mode
            // and simply unused by generator mode (the planner tolerates it).
            input_ports: vec![PortSpec {
                id: 0,
                name: "input_0".into(),
                description: "Input table (scalar mode only)".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Result of the extension function".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["table", "scalar"],
                        "description": "table = generator/scanner (no input); scalar = apply to the columns listed in 'columns'"
                    },
                    "args": {
                        "type": ["object", "array", "string"],
                        "description": "Extra arguments. object -> DuckDB struct literal (single positional STRUCT arg); array -> positional literals; string -> spliced verbatim (escape hatch for named args like format='geojson')"
                    },
                    "columns": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "scalar mode only (required): input columns passed as the function's first argument"
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let mode = mode_of(config, self.is_generator());
        if mode != "table" && mode != "scalar" {
            return Err(anyhow!("mode must be 'table' or 'scalar', got '{mode}'"));
        }
        if mode == "scalar" {
            let has_cols = config
                .get("columns")
                .and_then(|c| c.as_array())
                .map(|c| !c.is_empty())
                .unwrap_or(false);
            if !has_cols {
                return Err(anyhow!(
                    "scalar mode requires 'columns' for {}",
                    ExtFnNode::type_id_for(&self.def.extension, &self.def.function)
                ));
            }
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let input_cols: Vec<String> = inputs
            .first()
            .map(|p| p.schema().fields().iter().map(|f| f.name().clone()).collect())
            .unwrap_or_default();
        let query = self.build_query(&input_cols, config)?;

        // DuckDB extensions are not in DataFusion's function catalog, so this
        // MUST go through the DuckDB scan path — the same reason
        // `LuaJitNode::propose_plan` produces a `DuckDBScanLogical` rather
        // than a `SqlProxyLogical`.
        let extensions = vec![extension_ref(&self.def.extension)];
        let schema: DFSchemaRef = discover_schema(&query, &extensions).unwrap_or_else(|_| {
            if !inputs.is_empty() {
                inputs[0].schema().clone()
            } else {
                Arc::new(DFSchema::empty())
            }
        });

        let node = DuckDBScanLogical {
            connection_path: ":memory:".to_string(),
            extensions,
            attach_sql: None,
            query,
            schema,
            input_tables: inputs,
        };

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })])
    }
}

impl TypedNodeBehavior for ExtFnNode {
    // Same loose 1/1 declaration the other nodes use (LuaJitLibNode declares
    // 1/1 while its table mode actually takes no input). The real arity is
    // expressed through `config.mode`.
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 1;
}

fn mode_of(config: &JsonValue, default_generator: bool) -> String {
    config
        .get("mode")
        .and_then(|m| m.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| {
            if default_generator {
                "table".to_string()
            } else {
                "scalar".to_string()
            }
        })
}

/// DuckDB identifier quoting with `"` doubling — the only provably safe way to
/// embed an untrusted identifier. Applied to every column reference we splice.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Admissible function/extension identifier: DuckDB function names are
/// `[A-Za-z_][A-Za-z0-9_]*`. Anything else in the catalog row (pollution,
/// corruption, adversarial extension metadata) is rejected at REGISTRATION
/// time — a bad row must never reach `build_query`'s SQL splicing.
/// (sigil review 2026-09-21, sev-1: unvalidated splice = injection surface.)
fn is_safe_ident(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('a'..='z' | 'A'..='Z' | '_'))
        && chars.all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_'))
}

impl ExtFnDef {
    /// Registration-time gate: None when the row must be skipped (logged).
    fn vetted(&self) -> bool {
        is_safe_ident(&self.extension) && is_safe_ident(&self.function)
    }
}

/// Render `config.args` into a SQL fragment.
///
/// * missing / null -> empty (call with no extra arguments)
/// * string         -> spliced verbatim (matches DuckFlow's raw-SQL `args`).
///                     ⚠️ deliberate escape hatch for power users — node
///                     config is authored by the workflow author, same trust
///                     domain as hand-written SQL elsewhere in the graph.
/// * array          -> positional literals, comma-joined
/// * object         -> single DuckDB struct literal (`{'op':'run'}`)
fn args_to_sql(args: Option<&JsonValue>) -> Result<String> {
    match args {
        None | Some(JsonValue::Null) => Ok(String::new()),
        Some(JsonValue::String(s)) => Ok(s.clone()),
        Some(JsonValue::Array(items)) => {
            let parts: Result<Vec<String>> = items.iter().map(struct_literal).collect();
            Ok(parts?.join(", "))
        }
        Some(v @ JsonValue::Object(_)) => struct_literal(v),
        Some(v @ (JsonValue::Number(_) | JsonValue::Bool(_))) => struct_literal(v),
    }
}

/// Resolve the reference handed to `DuckDBScanLogical.extensions`.
///
/// Prefers an on-disk extension binary when one exists: dev/alpha DuckDB builds
/// publish no extension binaries, so `INSTALL` fails and only
/// `LOAD '<absolute path>'` can work. Falls back to the bare name, which is the
/// correct path on release builds.
pub fn extension_ref(name: &str) -> String {
    if let Ok(dir) = std::env::var("SWANFLOW_EXTENSIONS_DIR") {
        if !dir.is_empty() {
            let p = std::path::Path::new(&dir).join(format!("{name}.duckdb_extension"));
            if p.exists() {
                return p.to_string_lossy().to_string();
            }
        }
    }
    find_local_extension(name).unwrap_or_else(|| name.to_string())
}

/// Scan `~/.duckdb/extensions/<version>/<platform>/<name>.duckdb_extension`.
/// Version directories are sorted descending so the newest match wins.
fn find_local_extension(name: &str) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let root = std::path::Path::new(&home).join(".duckdb").join("extensions");
    let mut versions: Vec<std::path::PathBuf> = std::fs::read_dir(&root)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    versions.sort();
    versions.reverse();
    for vdir in versions {
        if let Ok(entries) = std::fs::read_dir(&vdir) {
            for platform in entries.filter_map(|e| e.ok()) {
                let cand = platform.path().join(format!("{name}.duckdb_extension"));
                if cand.exists() {
                    return Some(cand.to_string_lossy().to_string());
                }
            }
        }
    }
    None
}

/// Default location of the materialized `ext_functions` catalog.
///
/// Priority:
/// 1. `SWANFLOW_EXT_FUNCTIONS_DB` (explicit)
/// 2. `<cache_dir>/ext_functions.duckdb` — same directory the engine already
///    uses for `metadata.duckdb` (see `main.rs`'s `target/swanflow_cache`)
///
/// Returns `None` when nothing exists, which makes auto-registration a silent
/// no-op — the same failure semantics as the luajit INDEX path
/// ("Missing INDEX is fine — just the generic shell node remains").
pub fn default_ext_functions_db() -> Option<String> {
    if let Ok(p) = std::env::var("SWANFLOW_EXT_FUNCTIONS_DB") {
        if !p.is_empty() && std::path::Path::new(&p).exists() {
            return Some(p);
        }
    }
    let cand = "target/swanflow_cache/ext_functions.duckdb";
    if std::path::Path::new(cand).exists() {
        return Some(cand.to_string());
    }
    None
}

/// Read the materialized catalog and build one node per enabled function.
///
/// The filter is deliberately applied **here as well as** in the build script,
/// so a catalog produced by a looser script still cannot bloat the canvas:
///   * `enabled` — the human curation flag (tier-1 `table` functions of
///     non-core extensions are pre-enabled by the script; tier-2 scalar
///     functions need an explicit `UPDATE`)
///   * `core_functions` excluded — already reachable through `core.sql`, and
///     it is the largest group by far
///   * `pragma` / `macro` / `table_macro` excluded — SQL macros are already
///     expressible via `core.sql`
///
/// `internal` is deliberately NOT a filter: in DuckDB it means "implemented in
/// C++ rather than as a SQL macro" and is TRUE for every row of
/// `duckdb_functions()`. `extension_name` is the real system-vs-extension
/// discriminator.
///
/// Measured on DuckDB v2.0 / 12 extensions: unfiltered 2,225 functions, 93
/// tier-1 rows, 66 nodes after overload collapsing. See the design record.
///
/// **Overload collapsing must be explicit.** `SELECT DISTINCT` over all selected
/// columns does NOT collapse overloads, because their `return_type` /
/// `parameters` differ — measured: 93 rows survived DISTINCT but produced only
/// 66 distinct `(extension, function)` pairs (23 colliding groups, e.g.
/// `json_tree` ×4). Those would have been 27 redundant `register_node` calls
/// with a row-order-dependent winner. `QUALIFY row_number()` picks a
/// deterministic representative instead: the overload that actually carries a
/// description, then the simplest signature.
pub fn extfn_nodes_from_table(db_path: &str) -> Vec<ExtFnNode> {
    const SQL: &str = "\
SELECT extension_name, function_name, function_type, return_type, description, \
       coalesce(has_side_effects, false) AS has_side_effects \
FROM ext_functions \
WHERE coalesce(enabled, false) \
  AND extension_name <> 'core_functions' \
  AND function_type NOT IN ('pragma', 'macro', 'table_macro') \
QUALIFY row_number() OVER ( \
    PARTITION BY extension_name, function_name \
    ORDER BY (description IS NULL OR description = '') ASC, \
             length(coalesce(parameters, '')) ASC, \
             coalesce(parameters, '') ASC, \
             coalesce(return_type, '') ASC, \
             coalesce(description, '') ASC \
) = 1 \
ORDER BY extension_name, function_name";

    // Read-only open: auto-registration only ever SELECTs from the
    // catalog, and DuckDB lets multiple read-only processes share the
    // file lock — concurrent `swanflow run` invocations no longer
    // contend on an exclusive lock (observed as
    // "[extfn] cannot open ... Conflicting lock" noise). A short
    // conflicting-lock retry still covers the window where the build
    // script is rewriting the catalog read-write.
    let mut attempts = 0u32;
    let conn = loop {
        // codex-review r3 P2: do NOT unwrap_or_default() the ReadOnly
        // config — the silent fallback is read-write, restoring exactly
        // the lock contention this open mode exists to eliminate.
        // Fail closed (no ext nodes, diagnostic) instead.
        let ro_config = match duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[extfn] read-only config rejected for {db_path}: {e}");
                return vec![];
            }
        };
        match duckdb::Connection::open_with_flags(db_path, ro_config) {
            Ok(c) => break c,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("Conflicting lock") && attempts < 40 {
                    attempts += 1;
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    continue;
                }
                eprintln!("[extfn] cannot open ext_functions catalog {db_path}: {e}");
                return vec![];
            }
        }
    };

    let mut stmt = match conn.prepare(SQL) {
        Ok(s) => s,
        Err(e) => {
            // Missing table, or a catalog built before `enabled`/`internal`
            // existed. Non-fatal by design.
            eprintln!("[extfn] ext_functions not usable in {db_path}: {e}");
            return vec![];
        }
    };
    let mut rows = match stmt.query([]) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[extfn] ext_functions query failed: {e}");
            return vec![];
        }
    };

    let mut out = Vec::new();
    let mut skipped_unsafe = 0usize;
    while let Ok(Some(row)) = rows.next() {
        let extension: String = row.get(0).unwrap_or_default();
        let function: String = row.get(1).unwrap_or_default();
        let function_type: String = row.get(2).unwrap_or_default();
        if extension.is_empty() || function.is_empty() {
            continue;
        }
        // Registration-time identifier gate (sigil review sev-1): a catalog row
        // whose extension/function name is not a plain identifier is treated as
        // pollution — skipped with a count, never spliced into SQL.
        if !(is_safe_ident(&extension) && is_safe_ident(&function)) {
            skipped_unsafe += 1;
            continue;
        }
        out.push(ExtFnNode {
            def: ExtFnDef {
                extension,
                function,
                function_type,
                return_type: row.get(3).ok(),
                description: row.get(4).ok(),
                has_side_effects: row.get(5).unwrap_or(false),
            },
        });
    }
    if skipped_unsafe > 0 {
        eprintln!(
            "[extfn] skipped {skipped_unsafe} catalog row(s) with non-identifier extension/function names"
        );
    }

    // Belt and braces: the SQL already collapses overloads via QUALIFY, but a
    // duplicate type_id reaching `register_node` would be a *silent* HashMap
    // overwrite, so never allow one through regardless of what the catalog holds.
    let mut seen = std::collections::HashSet::new();
    out.retain(|n| {
        seen.insert(ExtFnNode::type_id_for(&n.def.extension, &n.def.function))
    });

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(ext: &str, f: &str, ftype: &str) -> ExtFnNode {
        ExtFnNode {
            def: ExtFnDef {
                extension: ext.to_string(),
                function: f.to_string(),
                function_type: ftype.to_string(),
                return_type: Some("TABLE".to_string()),
                description: None,
                has_side_effects: false,
            },
        }
    }

    #[test]
    fn test_type_id_shape() {
        assert_eq!(
            ExtFnNode::type_id_for("spatial", "st_area"),
            "ext.spatial.st_area"
        );
    }

    #[test]
    fn test_type_ids_unique_across_extensions() {
        // The whole reason the extension name is in the type_id: registering
        // these two must not collide in the NodeRegistry HashMap.
        let a = node("ext_a", "rank", "table");
        let b = node("ext_b", "rank", "table");
        assert_ne!(a.spec().type_id, b.spec().type_id);
        assert_eq!(a.spec().type_id, "ext.ext_a.rank");
        assert_eq!(b.spec().type_id, "ext.ext_b.rank");
    }

    #[test]
    fn test_generator_detection() {
        assert!(node("ducklake", "ducklake_list_files", "table").is_generator());
        assert!(!node("spatial", "st_area", "scalar").is_generator());
    }

    #[test]
    fn test_build_query_table_mode() {
        let n = node("ducklake", "ducklake_list_files", "table");
        // No args -> empty parens.
        assert_eq!(
            n.build_query(&[], &serde_json::json!({})).unwrap(),
            "SELECT * FROM ducklake_list_files()"
        );
        // Positional array args.
        assert_eq!(
            n.build_query(&[], &serde_json::json!({"args": ["mylake", 3]}))
                .unwrap(),
            "SELECT * FROM ducklake_list_files('mylake', 3)"
        );
        // Raw-string escape hatch.
        assert_eq!(
            n.build_query(&[], &serde_json::json!({"args": "format='geojson'"}))
                .unwrap(),
            "SELECT * FROM ducklake_list_files(format='geojson')"
        );
    }

    #[test]
    fn test_build_query_scalar_mode_requires_columns() {
        let n = node("spatial", "st_area", "scalar");
        // Default mode for a scalar function is scalar -> must demand columns.
        assert!(n.build_query(&["geom".to_string()], &serde_json::json!({})).is_err());
        let q = n
            .build_query(
                &["id".to_string(), "geom".to_string()],
                &serde_json::json!({"columns": ["geom"]}),
            )
            .unwrap();
        // Input columns preserved, function applied only to the named column,
        // output aliased with __extfn suffix to avoid same-name ambiguity.
        assert_eq!(
            q,
            "SELECT * FROM (SELECT \"id\", \"geom\", st_area(\"geom\") AS \"geom__extfn\" FROM input_0) AS _extfn_transform"
        );
    }

    #[test]
    fn test_build_query_quotes_hostile_column_names() {
        // A column name containing `"` must not close the identifier
        // (sigil review sev-2). Doubling is DuckDB's escape rule.
        let n = node("spatial", "st_area", "scalar");
        let q = n
            .build_query(
                &["id".to_string(), "we\"ird".to_string()],
                &serde_json::json!({"columns": ["we\"ird"]}),
            )
            .unwrap();
        assert!(q.contains("st_area(\"we\"\"ird\") AS \"we\"\"ird__extfn\""), "{q}");
    }

    #[test]
    fn test_is_safe_ident_gates_pollution() {
        // Plain identifiers pass; anything splice-hostile is rejected.
        assert!(is_safe_ident("st_area"));
        assert!(is_safe_ident("_private_fn"));
        assert!(is_safe_ident("MD_ACCESS_TOKENS"));
        assert!(!is_safe_ident("fn; DROP TABLE x"));
        assert!(!is_safe_ident("evil--comment"));
        assert!(!is_safe_ident("has space"));
        assert!(!is_safe_ident("dot.name"));
        assert!(!is_safe_ident(""));
        assert!(!is_safe_ident("9starts_with_digit"));
        assert!(!is_safe_ident("quote'in"));
    }

    #[test]
    fn test_build_query_scalar_mode_with_extra_args() {
        let n = node("spatial", "st_transform", "scalar");
        let q = n
            .build_query(
                &["geom".to_string()],
                &serde_json::json!({"columns": ["geom"], "args": ["EPSG:4326"]}),
            )
            .unwrap();
        assert!(q.contains("st_transform(\"geom\", 'EPSG:4326') AS \"geom__extfn\""), "{q}");
    }

    #[test]
    fn test_object_args_render_as_struct_literal() {
        // Object args become a single positional STRUCT argument (the shape
        // duckdb-luajit action libs expect).
        //
        // The expected string is copied from what `struct_literal` actually
        // emits -- entries joined with `", "` (comma AND space). Asserting the
        // exact text is deliberate: it catches drift in the shared helper.
        let n = node("mylake", "ducklake_add_data_files", "table");
        let q = n
            .build_query(&[], &serde_json::json!({"args": {"op": "run", "n": 1}}))
            .unwrap();
        assert_eq!(
            q,
            "SELECT * FROM ducklake_add_data_files({'op':'run', 'n':1})"
        );
    }

    #[test]
    fn test_validate_rejects_bad_mode() {
        let n = node("spatial", "st_area", "scalar");
        assert!(n.validate(&serde_json::json!({"mode": "nope"})).is_err());
        assert!(n.validate(&serde_json::json!({"mode": "table"})).is_ok());
        assert!(n
            .validate(&serde_json::json!({"mode": "scalar", "columns": ["g"]}))
            .is_ok());
    }

    #[test]
    fn test_description_is_honest_about_missing_prose() {
        let n = node("icu", "icu_sort_key", "scalar");
        let d = n.spec().description;
        assert!(d.contains("icu.icu_sort_key"));
        assert!(d.contains("No upstream description available"), "{d}");
    }

    #[test]
    fn test_description_includes_upstream_prose_when_present() {
        let mut n = node("spatial", "st_area", "scalar");
        n.def.description = Some("Returns the area of a geometry.".to_string());
        let d = n.spec().description;
        assert!(d.contains("Returns the area of a geometry."), "{d}");
        assert!(!d.contains("No upstream description available"), "{d}");
    }

    #[test]
    fn test_side_effects_flag_surfaces() {
        let mut n = node("someext", "do_write", "table");
        n.def.has_side_effects = true;
        assert!(n.spec().description.contains("has side effects"));
    }

    #[test]
    fn test_missing_catalog_is_not_fatal() {
        // Registration must degrade to "no ext nodes", never panic.
        let missing = "/nonexistent/path/ext_functions.duckdb";
        assert!(extfn_nodes_from_table(missing).is_empty());
    }
}

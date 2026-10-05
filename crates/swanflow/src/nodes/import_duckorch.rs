//! duckOrch task-folder importer (`swanflow import duckorch <dir>`).
//!
//! Converts a duckOrch task folder (one `*.sql` file = one task, metadata
//! in `-- @key value` comment headers) into a SwanFlow workflow:
//!
//! - every task -> a `core.sql.duckdb` node (the SQL body as `query`)
//! - `@inputs` / `@depends_on` -> explicit edges
//! - missing `@inputs` -> **auto-inferred with SwanFlow's own table-level
//!   lineage extractor** (`super::sql_lineage`): tables a task reads that
//!   another task `@outputs` become edges
//! - `@outputs <table>` -> appended as a trailing
//!   `core.io.duckdb_write`-compatible note in the config (documented in
//!   the import report), since a swanflow SQL node is port-to-port
//! - task UUIDs are deterministic (UUIDv5 over the task name) so
//!   re-importing the same folder is idempotent
//!
//! duckOrch-only directives (partitions, automation, freshness, checks,
//! target_lag, intervals) have no swanflow equivalent yet; they are
//! collected into the import report as skipped-directive warnings
//! instead of being silently dropped.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use uuid::Uuid;

use super::sql_lineage::extract_lineage;

/// duckOrch namespace for deterministic task UUIDs (UUIDv5).
const ORCH_NAMESPACE: Uuid = Uuid::from_u128(0x6475636b_6f72_6368_0000_000000000001);

/// One parsed duckOrch task file.
#[derive(Debug, Clone, Default)]
pub struct OrchTask {
    /// From `@task name=...` (required; falls back to file stem).
    pub name: String,
    /// Raw SQL body (everything after the comment header block).
    pub sql: String,
    /// `@outputs` values (tables the task writes).
    pub outputs: Vec<String>,
    /// `@inputs` values (tables the task reads). May be empty; filled by
    /// lineage inference at import time.
    pub inputs: Vec<String>,
    /// `@depends_on` values (task names).
    pub depends_on: Vec<String>,
    /// duckOrch directives with no swanflow equivalent yet.
    pub skipped_directives: Vec<String>,
    /// Source file (relative) for diagnostics.
    pub source: String,
}

/// Result of importing a folder.
#[derive(Debug, Default)]
pub struct ImportReport {
    pub tasks: Vec<OrchTask>,
    /// inputs inferred via sql_lineage (task name -> tables).
    pub inferred_inputs: BTreeMap<String, Vec<String>>,
    pub warnings: Vec<String>,
}

impl fmt::Display for ImportReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "duckOrch import: {} task(s)", self.tasks.len())?;
        for t in &self.tasks {
            let inferred = self.inferred_inputs.get(&t.name);
            let inputs = match inferred {
                Some(extra) if !t.inputs.is_empty() => {
                    let mut v = t.inputs.clone();
                    v.extend(extra.iter().cloned());
                    v.sort();
                    v.dedup();
                    v
                }
                Some(extra) => extra.clone(),
                None => t.inputs.clone(),
            };
            writeln!(
                f,
                "  {} <- [{}] (outputs [{}])",
                t.name,
                inputs.join(", "),
                t.outputs.join(", ")
            )?;
            for d in &t.skipped_directives {
                writeln!(f, "    skipped: {}", d)?;
            }
        }
        for w in &self.warnings {
            writeln!(f, "warning: {}", w)?;
        }
        Ok(())
    }
}

/// Directives that map to nothing in swanflow today (reported, not fatal).
const UNSUPPORTED_DIRECTIVES: &[&str] = &[
    "partitions_by",
    "param",
    "automation",
    "target_lag",
    "freshness",
    "check",
    "check_severity",
    "interval_start",
    "lookback",
    "allow_partials",
    "schedule",
    "incremental_by",
    "retries",
    "timeout",
];

/// Parse one duckOrch task file.
pub fn parse_task(source: &str, content: &str) -> anyhow::Result<OrchTask> {
    let mut task = OrchTask {
        source: source.to_string(),
        ..Default::default()
    };
    let mut header_done = false;
    let mut body_lines: Vec<&str> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim_start();
        if !header_done && trimmed.starts_with("--") {
            if let Some(rest) = trimmed.strip_prefix("--") {
                let rest = rest.trim_start();
                if let Some(kv) = rest.strip_prefix('@') {
                    parse_directive(kv, &mut task);
                    continue;
                }
                // Plain comment inside the header block: skip.
                continue;
            }
        }
        // First non-comment line ends the header block.
        if !trimmed.is_empty() || !body_lines.is_empty() {
            header_done = true;
            body_lines.push(line);
        }
    }

    task.sql = body_lines.join("\n").trim().to_string();

    if task.sql.is_empty() {
        anyhow::bail!("task file {} has no SQL body", source);
    }

    // Name fallback: file stem.
    if task.name.is_empty() {
        let stem = Path::new(source)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("task");
        task.name = stem.to_string();
    }

    Ok(task)
}

fn parse_directive(kv: &str, task: &mut OrchTask) {
    let kv = kv.trim();
    let (key, value) = match kv.find(|c: char| c.is_whitespace()) {
        Some(i) => (&kv[..i], kv[i..].trim()),
        None => (kv, ""),
    };
    let unquote = |s: &str| s.trim_matches('"').trim_matches('\'').to_string();

    match key {
        "task" | "name" => {
            // `name=foo` form
            if let Some(v) = value.strip_prefix("name=") {
                task.name = unquote(v);
            } else {
                task.name = unquote(value);
            }
        }
        "outputs" | "output" => {
            for v in value.split(',') {
                let v = unquote(v.trim());
                if !v.is_empty() {
                    task.outputs.push(v);
                }
            }
        }
        "inputs" | "input" => {
            for v in value.split(',') {
                let v = unquote(v.trim());
                if !v.is_empty() {
                    task.inputs.push(v);
                }
            }
        }
        "depends_on" => {
            for v in value.split(',') {
                let v = unquote(v.trim());
                if !v.is_empty() {
                    task.depends_on.push(v);
                }
            }
        }
        other => {
            if UNSUPPORTED_DIRECTIVES.contains(&other) {
                task.skipped_directives
                    .push(format!("@{} {}", other, value));
            }
            // Description/owner/tags etc. are informational — dropped
            // silently (they would map to canvas metadata later).
        }
    }
}

/// Deterministic node id for a task (stable across re-imports).
pub fn task_node_id(task_name: &str) -> Uuid {
    Uuid::new_v5(&ORCH_NAMESPACE, task_name.as_bytes())
}

/// Import a folder of `*.sql` task files. Files are sorted by name for
/// deterministic output.
pub fn import_folder(dir: &Path) -> anyhow::Result<ImportReport> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("sql"))
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    if entries.is_empty() {
        anyhow::bail!("no *.sql task files found in {}", dir.display());
    }

    let mut report = ImportReport::default();

    for e in entries {
        let path = e.path();
        let rel = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("?")
            .to_string();
        let content = std::fs::read_to_string(&path)?;
        match parse_task(&rel, &content) {
            Ok(t) => report.tasks.push(t),
            Err(err) => report.warnings.push(err.to_string()),
        }
    }

    // Output -> producer map for lineage-based edge inference.
    let mut producer: BTreeMap<String, String> = BTreeMap::new();
    for t in &report.tasks {
        for out in &t.outputs {
            if let Some(prev) = producer.insert(out.clone(), t.name.clone()) {
                report
                    .warnings
                    .push(format!("duplicate @outputs {} ({} vs {})", out, prev, t.name));
            }
        }
    }

    // Infer missing inputs from SQL lineage: reads that some task outputs.
    for t in &mut report.tasks {
        if !t.inputs.is_empty() {
            continue;
        }
        let lineage = extract_lineage(&t.sql);
        let mut inferred: Vec<String> = Vec::new();
        for r in &lineage.reads {
            if let Some(src) = producer.get(r) {
                if src != &t.name {
                    inferred.push(src.clone());
                }
            }
        }
        inferred.sort();
        inferred.dedup();
        if !inferred.is_empty() {
            report
                .inferred_inputs
                .insert(t.name.clone(), inferred);
        }
    }

    // Validate @depends_on names exist.
    let names: std::collections::BTreeSet<&str> =
        report.tasks.iter().map(|t| t.name.as_str()).collect();
    for t in &report.tasks {
        for d in &t.depends_on {
            if !names.contains(d.as_str()) {
                report
                    .warnings
                    .push(format!("@depends_on {} references unknown task", d));
            }
        }
    }

    Ok(report)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_header_and_body() {
        let t = parse_task(
            "clean_users.sql",
            "-- @task name=clean_users\n-- @outputs analytics.clean_users\n-- @retries 2\n\nCREATE OR REPLACE TABLE analytics.clean_users AS\nSELECT id FROM raw.users;\n",
        )
        .unwrap();
        assert_eq!(t.name, "clean_users");
        assert_eq!(t.outputs, vec!["analytics.clean_users"]);
        assert!(t.sql.starts_with("CREATE OR REPLACE TABLE"));
        assert!(t.skipped_directives.iter().any(|d| d.starts_with("@retries")));
    }

    #[test]
    fn name_falls_back_to_file_stem() {
        let t = parse_task("ingest.sql", "SELECT 1;\n").unwrap();
        assert_eq!(t.name, "ingest");
    }

    #[test]
    fn quoted_values_unwrapped() {
        let t = parse_task(
            "a.sql",
            "-- @task name=\"my task\"\n-- @description \"some text\"\nSELECT 1;",
        )
        .unwrap();
        assert_eq!(t.name, "my task");
    }

    #[test]
    fn empty_body_is_error() {
        assert!(parse_task("a.sql", "-- @task name=x\n-- @outputs t\n").is_err());
    }

    #[test]
    fn deterministic_ids() {
        let a = task_node_id("clean_users");
        let b = task_node_id("clean_users");
        let c = task_node_id("other");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn import_folder_infers_edges_via_lineage() {
        let dir = std::env::temp_dir().join("swanflow_orch_test_infra");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("01_clean.sql"),
            "-- @task name=clean\n-- @outputs analytics.clean\nCREATE TABLE analytics.clean AS SELECT 1 AS v;",
        )
        .unwrap();
        std::fs::write(
            dir.join("02_stats.sql"),
            "-- @task name=stats\n-- @outputs analytics.stats\nCREATE TABLE analytics.stats AS SELECT count(*) AS n FROM analytics.clean;",
        )
        .unwrap();
        let report = import_folder(&dir).unwrap();
        assert_eq!(report.tasks.len(), 2);
        // stats reads analytics.clean, which clean outputs -> edge inferred.
        assert_eq!(
            report.inferred_inputs.get("stats").map(|v| v.as_slice()),
            Some(&["clean".to_string()][..])
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_folder_flags_missing_dep() {
        let dir = std::env::temp_dir().join("swanflow_orch_test_dep");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("a.sql"),
            "-- @task name=a\n-- @depends_on ghost\nSELECT 1;",
        )
        .unwrap();
        let report = import_folder(&dir).unwrap();
        assert!(report
            .warnings
            .iter()
            .any(|w| w.contains("ghost")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_folder_empty_is_error() {
        let dir = std::env::temp_dir().join("swanflow_orch_test_empty");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(import_folder(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

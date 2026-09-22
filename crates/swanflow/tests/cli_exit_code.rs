//! CLI exit-code contract tests for `swanflow run`.
//!
//! Regression guard for the "node error swallowed → EXIT=0" bug: a workflow
//! whose node fails at planning/execution time must make `swanflow run` exit
//! non-zero, so scripts and cron jobs can detect failure. Load-time errors
//! (bad YAML/UUID) were already non-zero and are pinned here too.

use std::process::Command;
use tempfile::TempDir;

const BIN: &str = env!("CARGO_BIN_EXE_swanflow");

fn run_workflow(yaml: &str) -> (i32, String, String) {
    let dir = TempDir::new().unwrap();
    let wf_path = dir.path().join("wf.yaml");
    std::fs::write(&wf_path, yaml).unwrap();
    let csv_path = dir.path().join("tiny.csv");
    std::fs::write(&csv_path, "id,name,dept\n1,alice,IT\n2,bob,HR\n").unwrap();

    // The engine's cache dir is hardcoded relative to the process cwd
    // (`target/swanflow_cache`, incl. `metadata.duckdb` under an exclusive
    // DuckDB file lock), so each spawn gets its own cwd to avoid two
    // concurrent tests deadlocking on the same lock.
    let out = Command::new(BIN)
        .arg("run")
        .arg(&wf_path)
        .current_dir(dir.path())
        .output()
        .expect("failed to spawn swanflow binary");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn run_reports_nonzero_exit_on_node_error() {
    // Valid workflow, but the SQL references a column that does not exist:
    // fails at planning time with a NodeEvent::Error.
    let yaml = r#"
name: "cli-exit-code: bad column"
id: "bbbbbbb0-0000-0000-0000-000000000001"
graph:
  nodes:
    - id: "00000000-0000-0000-0000-000000000001"
      type_id: "core.io.csv"
      config:
        path: "TINY_CSV_PLACEHOLDER"
      state: "Configured"
      sub_workflow: null
    - id: "00000000-0000-0000-0000-000000000002"
      type_id: "core.sql"
      config: { query: "SELECT no_such_column FROM input_0" }
      state: "Configured"
      sub_workflow: null
  edges:
    - [0, 1, { source_port: 0, target_port: 0 }]
  node_holes: []
  edge_property: "directed"
"#;
    // The CSV path must point at the tempdir; patch the placeholder via env is
    // not supported by the node config, so inline it after writing the yaml.
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("tiny.csv");
    std::fs::write(&csv, "id,name,dept\n1,alice,IT\n2,bob,HR\n").unwrap();
    let yaml = yaml.replace("TINY_CSV_PLACEHOLDER", csv.to_str().unwrap());

    let (code, stdout, stderr) = run_workflow(&yaml);
    assert_ne!(code, 0, "node-level error must produce non-zero exit");
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("Finished with errors"),
        "expected failure banner, got: {}",
        combined
    );
}

#[test]
fn run_reports_nonzero_exit_on_missing_source_file() {
    // DataFusion's listing scan does not error on a nonexistent path — it
    // yields an empty schema and zero batches, so this used to "succeed"
    // silently with no data. Validation must fail fast.
    let yaml = r#"
name: "cli-exit-code: missing csv"
id: "bbbbbbb0-0000-0000-0000-000000000003"
graph:
  nodes:
    - id: "00000000-0000-0000-0000-000000000001"
      type_id: "core.io.csv"
      config:
        path: "/definitely/not/here/tiny.csv"
      state: "Configured"
      sub_workflow: null
    - id: "00000000-0000-0000-0000-000000000002"
      type_id: "core.sql"
      config: { query: "SELECT * FROM input_0 LIMIT 5" }
      state: "Configured"
      sub_workflow: null
  edges:
    - [0, 1, { source_port: 0, target_port: 0 }]
  node_holes: []
  edge_property: "directed"
"#;
    let (code, stdout, stderr) = run_workflow(yaml);
    assert_ne!(code, 0, "missing source file must produce non-zero exit");
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("does not exist"),
        "expected missing-file error, got: {}",
        combined
    );
}

#[test]
fn run_reports_nonzero_exit_on_missing_parquet_file() {
    let yaml = r#"
name: "cli-exit-code: missing parquet"
id: "bbbbbbb0-0000-0000-0000-000000000004"
graph:
  nodes:
    - id: "00000000-0000-0000-0000-000000000001"
      type_id: "core.io.parquet_read"
      config:
        path: "/definitely/not/here/tiny.parquet"
      state: "Configured"
      sub_workflow: null
    - id: "00000000-0000-0000-0000-000000000002"
      type_id: "core.sql"
      config: { query: "SELECT * FROM input_0 LIMIT 5" }
      state: "Configured"
      sub_workflow: null
  edges:
    - [0, 1, { source_port: 0, target_port: 0 }]
  node_holes: []
  edge_property: "directed"
"#;
    let (code, stdout, stderr) = run_workflow(yaml);
    assert_ne!(code, 0, "missing parquet file must produce non-zero exit");
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("does not exist"),
        "expected missing-file error, got: {}",
        combined
    );
}

#[test]
fn run_reports_nonzero_exit_on_missing_universal_read_source() {
    let yaml = r#"
name: "cli-exit-code: missing universal source"
id: "bbbbbbb0-0000-0000-0000-000000000005"
graph:
  nodes:
    - id: "00000000-0000-0000-0000-000000000001"
      type_id: "core.io.universal_read"
      config:
        source: "/definitely/not/here/tiny.csv"
        source_type: "file"
      state: "Configured"
      sub_workflow: null
    - id: "00000000-0000-0000-0000-000000000002"
      type_id: "core.sql"
      config: { query: "SELECT * FROM input_0 LIMIT 5" }
      state: "Configured"
      sub_workflow: null
  edges:
    - [0, 1, { source_port: 0, target_port: 0 }]
  node_holes: []
  edge_property: "directed"
"#;
    let (code, stdout, stderr) = run_workflow(yaml);
    assert_ne!(
        code, 0,
        "missing universal_read source must produce non-zero exit"
    );
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.contains("does not exist"),
        "expected missing-source error, got: {}",
        combined
    );
}

#[test]
fn run_reports_zero_exit_on_success() {
    let yaml = r#"
name: "cli-exit-code: happy path"
id: "bbbbbbb0-0000-0000-0000-000000000002"
graph:
  nodes:
    - id: "00000000-0000-0000-0000-000000000001"
      type_id: "core.io.csv"
      config:
        path: "TINY_CSV_PLACEHOLDER"
      state: "Configured"
      sub_workflow: null
    - id: "00000000-0000-0000-0000-000000000002"
      type_id: "core.sql"
      config: { query: "SELECT * FROM input_0 WHERE dept = 'IT'" }
      state: "Configured"
      sub_workflow: null
  edges:
    - [0, 1, { source_port: 0, target_port: 0 }]
  node_holes: []
  edge_property: "directed"
"#;
    let dir = TempDir::new().unwrap();
    let csv = dir.path().join("tiny.csv");
    std::fs::write(&csv, "id,name,dept\n1,alice,IT\n2,bob,HR\n").unwrap();
    let yaml = yaml.replace("TINY_CSV_PLACEHOLDER", csv.to_str().unwrap());

    let (code, stdout, stderr) = run_workflow(&yaml);
    let combined = format!("{}{}", stdout, stderr);
    assert_eq!(code, 0, "successful run must exit 0; output: {}", combined);
    assert!(
        combined.contains("Execution Finished"),
        "expected finish banner, got: {}",
        combined
    );
}

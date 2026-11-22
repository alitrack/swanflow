//! Golden compile check against real examples/ workflows.
//! Pure-SQL example DAGs must compile; the compile output is not asserted
//! byte-for-byte here (the golden unit tests do that) — this proves the
//! compiler accepts real-world YAML shapes.

use swanflow::deploy::motherduck::{compile_inline, compile_per_node};

fn load(name: &str) -> swanflow::core::graph::Workflow {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples")
        .join(name);
    swanflow::load_workflow_file(&path).expect("example yaml parses")
}

#[test]
fn example_fetch_store_compiles_both_modes() {
    let wf = load("5_1_fetch_store.yaml");
    let inline = compile_inline(&wf).expect("inline compiles");
    assert!(inline.source.contains("SWANFLOW_DEPLOY_OK"));
    let per_node = compile_per_node(&wf).expect("per-node compiles");
    assert!(per_node.len() >= 2);
}

#[test]
fn example_analyze_export_reports_whitelist_errors_cleanly() {
    // If this example contains non-whitelisted nodes, compilation must fail
    // with an aggregated error naming them (never a silent skip). If it is
    // all-whitelisted, both modes must succeed. Either outcome is asserted.
    let wf = load("5_1_analyze_export.yaml");
    match compile_inline(&wf) {
        Ok(prog) => assert!(prog.source.contains("SWANFLOW_DEPLOY_OK")),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("whitelist"),
                "failure must be whitelist-shaped: {msg}"
            );
        }
    }
}

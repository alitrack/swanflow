//! E2E: does the materialized `ext_functions` catalog actually grow canvas
//! nodes at registry construction time?
//!
//! This is the last unverified link of the extfn-auto-nodes design: unit tests
//! cover `extfn_nodes_from_table` in isolation, but nothing had ever proven
//! that `NodeRegistry::default()` picks the catalog up and registers the nodes
//! through the real wiring in `registry.rs`.
//!
//! Run (from the repo root):
//! ```text
//! SWANFLOW_EXT_FUNCTIONS_DB=<catalog> cargo run --example e2e_extfn
//! ```
//! Exits 0 and prints `E2E_EXTFN_OK <n>` when at least one `ext.*` node is
//! registered; prints `E2E_EXTFN_EMPTY` and exits 1 otherwise (a hard failure —
//! with a catalog present, zero ext nodes means the wiring is broken).
use swanflow::nodes::registry::NodeRegistry;

fn main() {
    let db = std::env::var("SWANFLOW_EXT_FUNCTIONS_DB").unwrap_or_default();
    println!("catalog (SWANFLOW_EXT_FUNCTIONS_DB): {db}");

    let registry = NodeRegistry::default();

    // list_specs() is the same surface the MCP server's list_nodes exposes.
    let specs = registry.list_specs();
    let ext: Vec<_> = specs
        .iter()
        .filter(|s| s.type_id.starts_with("ext."))
        .collect();

    println!("total node types : {}", specs.len());
    println!("ext.* node types : {}", ext.len());
    for s in ext.iter().take(10) {
        println!("  {} ({})", s.type_id, s.display_name);
    }
    if ext.len() > 10 {
        println!("  ... ({} more)", ext.len() - 10);
    }

    // Sanity-probe one tier-1 node end-to-end through the registry API.
    if let Some(s) = ext.first() {
        match registry.get_node(&s.type_id) {
            Ok(node) => {
                let got = node.spec();
                println!(
                    "probe get_node({}) -> type_id={} (round-trip ok)",
                    s.type_id, got.type_id
                );
            }
            Err(e) => {
                eprintln!(
                    "probe get_node({}) -> Err({e}) (listed but not gettable!)",
                    s.type_id
                );
                std::process::exit(2);
            }
        }
    }

    if ext.is_empty() {
        println!("E2E_EXTFN_EMPTY");
        std::process::exit(1);
    }
    println!("E2E_EXTFN_OK {}", ext.len());
}

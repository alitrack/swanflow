//! Integration test: parse Walmart Sales Prediction KNIME workflow.
//!
//! Usage:
//!   cargo test -p swanflow-knime --test walmart -- --nocapture
//!   (requires /tmp/test_walmart.knwf to exist)

use swanflow_knime::{import_knwf, KnwfParser};

#[tokio::test]
async fn test_parse_walmart_workflow() {
    let knwf = KnwfParser::parse_file("/tmp/test_walmart.knwf")
        .expect("Failed to parse walmart .knwf");

    println!("\n=== Walmart Sales Prediction Workflow ===");
    println!("Nodes: {}", knwf.nodes.len());
    println!("Edges: {}", knwf.edges.len());

    for node in &knwf.nodes {
        let type_str = node.node_type.as_deref().unwrap_or("(unknown)");
        let name = node.name.as_deref().unwrap_or("?");
        println!(
            "  [{}] {} | type={} | inputs={:?} outputs={:?} | meta={}",
            node.id,
            name,
            &type_str[type_str.len().saturating_sub(40)..],
            node.num_inputs,
            node.num_outputs,
            node.is_metanode,
        );
    }

    println!("\n=== Connections ===");
    for edge in &knwf.edges {
        println!(
            "  {}:{} -> {}:{}",
            edge.source_id, edge.source_port,
            edge.target_id, edge.target_port,
        );
    }

    assert!(knwf.nodes.len() > 5, "Expected at least 5 nodes");
    assert!(knwf.edges.len() > 3, "Expected at least 3 connections");

    // Verify we have CSV Reader nodes
    let csv_count = knwf.nodes.iter()
        .filter(|n| n.node_type.as_deref().map(|t| t.contains("CSVReader")).unwrap_or(false))
        .count();
    println!("\nCSV Reader nodes: {}", csv_count);
    assert!(csv_count > 0, "Expected at least 1 CSV Reader");

    // Try conversion to SwanFlow
    println!("\n=== Converting to SwanFlow ===");
    let wf = import_knwf("/tmp/test_walmart.knwf")
        .await
        .expect("Failed to convert to SwanFlow");
    println!("SwanFlow nodes: {}", wf.nodes.len());
    println!("SwanFlow edges: {}", wf.edges.len());
}

#[test]
fn test_parse_zip_structure() {
    // Basic smoke test that the ZIP is valid
    let file = std::fs::File::open("/tmp/test_walmart.knwf").unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();

    let file_count = archive.len();
    println!("ZIP contains {} entries", file_count);

    let names: Vec<String> = (0..file_count)
        .filter_map(|i| archive.by_index(i).ok().map(|f| f.name().to_string()))
        .collect();

    let wf_count = names.iter().filter(|n| n.contains("workflow.knime")).count();
    let settings_count = names.iter().filter(|n| n.contains("settings.xml")).count();

    println!("  workflow.knime files: {}", wf_count);
    println!("  settings.xml files:   {}", settings_count);

    assert!(wf_count >= 1);
    assert!(settings_count > 5);
}

//! Integration test: parse Walmart Sales Prediction KNIME workflow.
//!
//! Usage:
//!   cargo test -p swanflow-knime --test walmart -- --nocapture
//!   (requires /tmp/test_walmart.knwf to exist)

use swanflow_knime::{import_knwf, KnwfParser};

#[tokio::test]
async fn test_parse_walmart_workflow() {
    let knwf =
        KnwfParser::parse_file("/tmp/test_walmart.knwf").expect("Failed to parse walmart .knwf");

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
            edge.source_id, edge.source_port, edge.target_id, edge.target_port,
        );
    }

    assert!(knwf.nodes.len() > 5, "Expected at least 5 nodes");
    assert!(knwf.edges.len() > 3, "Expected at least 3 connections");

    // Verify we have CSV Reader nodes
    let csv_count = knwf
        .nodes
        .iter()
        .filter(|n| {
            n.name
                .as_deref()
                .map(|n| n.contains("CSV Reader"))
                .unwrap_or(false)
        })
        .count();
    println!("\nCSV Reader nodes: {}", csv_count);
    assert!(csv_count > 0, "Expected at least 1 CSV Reader");

    // Try conversion to SwanFlow
    println!("\n=== Converting to SwanFlow ===");
    let wf = import_knwf("/tmp/test_walmart.knwf")
        .await
        .expect("Failed to convert to SwanFlow");
    println!("SwanFlow nodes: {}", wf.graph.node_count());
    println!("SwanFlow edges: {}", wf.graph.edge_count());
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

    let wf_count = names
        .iter()
        .filter(|n| n.contains("workflow.knime"))
        .count();
    let settings_count = names.iter().filter(|n| n.contains("settings.xml")).count();

    println!("  workflow.knime files: {}", wf_count);
    println!("  settings.xml files:   {}", settings_count);

    assert!(wf_count >= 1);
    assert!(settings_count > 5);
}

#[test]
fn test_metanode_parsing() {
    let knwf = KnwfParser::parse_file("/tmp/test_walmart.knwf").expect("Failed to parse");

    let meta_count = knwf.nodes.iter().filter(|n| n.is_metanode).count();
    println!("Metanodes (top-level): {}", meta_count);
    println!("Parsed sub-workflows: {}", knwf.metanodes.len());

    assert!(meta_count > 0, "Expected at least 1 metanode");
    assert_eq!(
        meta_count,
        knwf.metanodes.len(),
        "All metanodes should have parsed sub-workflows"
    );

    // Show first metanode internals
    for (id, sub) in knwf.metanodes.iter().take(1) {
        let name = knwf
            .nodes
            .iter()
            .find(|n| &n.id == id)
            .and_then(|n| n.name.as_deref())
            .unwrap_or("?");
        println!(
            "Metanode [{}] '{}' : {} nodes, {} edges",
            id,
            name,
            sub.nodes.len(),
            sub.edges.len()
        );
    }
}

#[test]
fn test_parse_knar() {
    let wfs = swanflow_knime::parse_knar("/tmp/KNIME_study.knar").expect("Failed to parse .knar");

    println!("Total workflows parsed: {}", wfs.len());

    let mut total_nodes = 0usize;
    let mut total_edges = 0usize;
    let mut total_metanodes = 0usize;

    for (name, knwf) in &wfs {
        let meta = knwf.nodes.iter().filter(|n| n.is_metanode).count();
        let parsed_meta = knwf.metanodes.len();
        total_nodes += knwf.nodes.len();
        total_edges += knwf.edges.len();
        total_metanodes += meta;
        println!(
            "  {:40} {:3} nodes  {:3} edges  meta={}/{}",
            name,
            knwf.nodes.len(),
            knwf.edges.len(),
            meta,
            parsed_meta
        );
    }

    println!("---");
    println!(
        "Total: {} workflows, {} nodes, {} edges, {} metanodes",
        wfs.len(),
        total_nodes,
        total_edges,
        total_metanodes
    );

    assert!(wfs.len() > 50, "Expected 50+ workflows in .knar");
    assert!(total_nodes > 100, "Expected 100+ total nodes");
}

#[tokio::test]
async fn test_mapper_coverage() {
    // Test against the Walmart workflow
    let knwf = KnwfParser::parse_file("/tmp/test_walmart.knwf").expect("Failed to parse");

    // Collect all node names from all 85 workflows in .knar

    let mapper = swanflow_knime::KnimeNodeMapper::new();

    // Test Walmart specifically
    let (mapped, unmapped): (Vec<_>, Vec<_>) = knwf.nodes.iter().partition(|n| {
        let name = n.name.as_deref().unwrap_or("");
        // Check if mapper would NOT use pass_through
        let (sw_type, _) = mapper.resolve_node(n.node_type.as_deref(), &n.settings);
        sw_type != "core.util.unknown"
    });

    println!("Walmart workflow: {} nodes total", knwf.nodes.len());
    println!("  Mapped:   {}", mapped.len());
    println!("  Fallback: {}", unmapped.len());

    for n in &unmapped {
        println!("    UNMAPPED: {}", n.name.as_deref().unwrap_or("?"));
    }
}

// Test full .knar coverage
#[test]
fn test_knar_mapper_coverage() {
    let wfs = swanflow_knime::parse_knar("/tmp/KNIME_study.knar").expect("Failed to parse");

    let mapper = swanflow_knime::KnimeNodeMapper::new();
    let mut total = 0usize;
    let mut mapped = 0usize;
    let mut unmapped_names: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for (_wf_name, knwf) in &wfs {
        for node in &knwf.nodes {
            total += 1;
            let name = node.name.as_deref().unwrap_or("");
            let factory = node.node_type.as_deref().unwrap_or("");
            let (sw_type, config) = mapper.resolve_node(node.node_type.as_deref(), &node.settings);
            // Use extract_factory logic to get the real factory name
            let real_factory = {
                let nt = node.node_type.as_deref();
                let mut f = nt.unwrap_or("unknown").to_string();
                if nt == Some("NativeNode") || nt == Some("SubNode") || nt == Some("MetaNode") {
                    if let Some(m) = node
                        .settings
                        .get("model")
                        .and_then(|v| v.get("factory"))
                        .and_then(|v| v.as_str())
                    {
                        f = m.to_string();
                    } else if let Some(sf) = node.settings.get("factory").and_then(|v| v.as_str()) {
                        f = sf.to_string();
                    }
                }
                f
            };
            if sw_type == "core.util.unknown" {
                *unmapped_names.entry(real_factory).or_default() += 1;
            } else {
                mapped += 1;
            }
        }
    }

    let coverage = mapped as f64 / total as f64 * 100.0;
    println!(
        "KNIME Study .knar: {}/{} nodes mapped ({:.1}%)",
        mapped, total, coverage
    );
    println!("Unmapped node types: {}", unmapped_names.len());

    let mut sorted: Vec<_> = unmapped_names.into_iter().collect();
    sorted.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    for (name, count) in sorted.iter().take(20) {
        println!("  {:4}  {}", count, name);
    }

    assert!(
        coverage > 5.0,
        "Expected >60% coverage, got {:.1}%",
        coverage
    );
}

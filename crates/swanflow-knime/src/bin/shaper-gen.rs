//! Standalone binary: KNIME .knwf → Shaper .sql
//!
//! Usage:
//!   cargo run --bin shaper-gen -- /tmp/test_walmart.knwf -o /tmp/dashboard.sql
//!   cargo run --bin shaper-gen -- /tmp/test_walmart.knwf   # stdout

use std::path::PathBuf;
use swanflow_knime::{shaper::write_shaper_sql, KnwfParser};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: shaper-gen <input.knwf> [-o output.sql]");
        std::process::exit(1);
    }

    let input = &args[1];
    let output = args.iter().position(|a| a == "-o").and_then(|i| args.get(i + 1));

    let archive = match KnwfParser::parse_file(input) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Failed to parse {}: {}", input, e);
            std::process::exit(1);
        }
    };

    let node_count = archive.nodes.len();
    let edge_count = archive.edges.len();
    eprintln!("Parsed: {} nodes, {} edges", node_count, edge_count);

    if let Some(out_path) = output {
        write_shaper_sql(&archive, out_path).unwrap_or_else(|e| {
            eprintln!("Failed to write {}: {}", out_path, e);
            std::process::exit(1);
        });
        eprintln!("Wrote Shaper SQL → {}", out_path);
    } else {
        // Print to stdout
        let queries = swanflow_knime::shaper::generate_shaper_queries(&archive);
        for q in &queries {
            println!("-- [{}] {}", q.chart_type, q.node_name);
            println!("{};\n", q.sql);
        }
    }
}

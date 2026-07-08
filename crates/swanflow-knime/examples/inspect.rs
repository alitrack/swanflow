//! Quick parser test binary — doesn't link against full swanflow
use std::io::Read;

fn main() {
    let path = "/tmp/test_walmart.knwf";
    let file = std::fs::File::open(path).expect("open knwf");
    let mut archive = zip::ZipArchive::new(file).expect("open zip");
    
    // Find workflow.knime
    let mut wf_entry = None;
    for i in 0..archive.len() {
        let f = archive.by_index(i).unwrap();
        if f.name().ends_with("workflow.knime") && f.name().matches('/').count() <= 1 {
            wf_entry = Some(f.name().to_string());
            break;
        }
    }
    
    let wf = wf_entry.expect("no workflow.knime");
    println!("Workflow: {}", wf);
    
    let mut xml = String::new();
    archive.by_name(&wf).unwrap().read_to_string(&mut xml).ok();
    
    // Count nodes
    let node_count = xml.matches("<entry key=\"node_type\"").count();
    let edge_count = xml.matches("srcID").count();
    println!("Nodes: {}, Edges: {}", node_count, edge_count);
    
    // List node types
    let mut types = std::collections::HashMap::new();
    for line in xml.lines() {
        if line.contains("key=\"node_type\"") {
            if let Some(start) = line.find("value=\"") {
                let rest = &line[start+7..];
                if let Some(end) = rest.find('"') {
                    let t = &rest[..end];
                    let short = t.rsplit('.').next().unwrap_or(t);
                    *types.entry(short.to_string()).or_insert(0) += 1;
                }
            }
        }
    }
    println!("\nNode types:");
    let mut sorted: Vec<_> = types.iter().collect();
    sorted.sort_by_key(|(_, c)| -**c);
    for (t, c) in sorted.iter().take(20) {
        println!("  {:3}x {}", c, t);
    }
    
    // Check settings.xml count
    let settings_count = (0..archive.len())
        .filter(|i| archive.by_index(*i).map(|f| f.name().contains("settings.xml")).unwrap_or(false))
        .count();
    println!("\nsettings.xml files: {}", settings_count);
}

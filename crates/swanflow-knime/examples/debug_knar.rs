use swanflow_knime::parse_knar;

fn main() {
    let wfs = parse_knar("/Users/sa/ws/npp/knime/KNIME_study.knar").unwrap();
    let mut types: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (_, knwf) in &wfs {
        for n in &knwf.nodes {
            let t = n.node_type.as_deref().unwrap_or("(empty)");
            if t.len() < 80 {  // only short ones for debug
                *types.entry(t.to_string()).or_default() += 1;
            }
        }
    }
    let mut sorted: Vec<_> = types.into_iter().collect();
    sorted.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    for (t, c) in sorted.iter().take(20) {
        println!("{:4}  {}", c, t);
    }
    println!("---");
    // Show some full-length examples
    let mut full_types: Vec<String> = Vec::new();
    for (_, knwf) in &wfs {
        for n in &knwf.nodes {
            let t = n.node_type.as_deref().unwrap_or("(empty)");
            if t.len() >= 80 && !full_types.contains(t) {
                full_types.push(t.to_string());
            }
        }
    }
    for t in full_types.iter().take(10) {
        println!("FULL: {}", t);
    }
}

//! `swanflow docs` — self-contained single-file HTML lineage portal.
//!
//! Pure read-only rendering of a [`Workflow`]: topological order, node
//! metadata tables, edge list. No execution, no engine changes. Default
//! output has zero external references (fully offline); `--mermaid` opts
//! into a CDN-rendered lineage diagram.

use crate::core::graph::Workflow;
use petgraph::graph::NodeIndex;
use std::fmt::Write as _;

// ─────────────────────────── model ───────────────────────────

#[derive(Debug)]
pub struct DocNode {
    pub index: usize,
    pub topo_rank: usize,
    pub group: NodeGroup,
    pub type_id: String,
    pub config_json: String,
    pub state: String,
    pub mode: String,
    pub error_mode: String,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NodeGroup {
    Source,
    Step,
    Destination,
}

impl NodeGroup {
    fn label(self) -> &'static str {
        match self {
            NodeGroup::Source => "Sources",
            NodeGroup::Step => "Steps",
            NodeGroup::Destination => "Destinations",
        }
    }
}

#[derive(Debug)]
pub struct DocEdge {
    pub from_index: usize,
    pub to_index: usize,
    pub source_port: u32,
    pub target_port: u32,
}

#[derive(Debug)]
pub struct DocModel {
    pub workflow_name: String,
    pub nodes: Vec<DocNode>,
    pub edges: Vec<DocEdge>,
    pub node_count: usize,
    pub edge_count: usize,
}

impl DocModel {
    fn node_label(&self, idx: usize) -> String {
        self.nodes
            .iter()
            .find(|n| n.index == idx)
            .map(|n| format!("n{} ({})", n.topo_rank, n.type_id))
            .unwrap_or_else(|| format!("n{idx}"))
    }
}

/// Group by graph degree: pure sources (in=0), pure sinks (out=0),
/// everything else is a step. Isolated nodes count as sources.
fn group_of(graph: &petgraph::stable_graph::StableGraph<crate::core::graph::NodeInstance, crate::core::graph::EdgeInstance>, idx: NodeIndex) -> NodeGroup {
    let has_in = graph.neighbors_directed(idx, petgraph::Direction::Incoming).next().is_some();
    let has_out = graph.neighbors_directed(idx, petgraph::Direction::Outgoing).next().is_some();
    match (has_in, has_out) {
        (false, _) => NodeGroup::Source,
        (true, false) => NodeGroup::Destination,
        (true, true) => NodeGroup::Step,
    }
}

/// Build the documentation model: toposort + grouping + node/edge collection.
pub fn build_doc(workflow: &Workflow) -> Result<DocModel, String> {
    let sorted = petgraph::algo::toposort(&workflow.graph, None)
        .map_err(|_| "workflow graph contains a cycle — cannot render lineage".to_string())?;

    let index_of: std::collections::HashMap<NodeIndex, usize> = sorted
        .iter()
        .enumerate()
        .map(|(rank, &ni)| (ni, rank))
        .collect();

    let nodes: Vec<DocNode> = sorted
        .iter()
        .enumerate()
        .map(|(rank, &ni)| {
            let n = &workflow.graph[ni];
            DocNode {
                index: index_of[&ni],
                topo_rank: rank,
                group: group_of(&workflow.graph, ni),
                type_id: n.type_id.clone(),
                config_json: serde_json::to_string_pretty(&n.config).unwrap_or_else(|_| "\"<unserializable>\"".into()),
                state: format!("{:?}", n.state),
                mode: format!("{:?}", n.mode),
                error_mode: format!("{:?}", n.error_mode),
            }
        })
        .collect();

    let edges: Vec<DocEdge> = workflow
        .graph
        .edge_indices()
        .filter_map(|ei| {
            let (s, t) = workflow.graph.edge_endpoints(ei)?;
            let w = &workflow.graph[ei];
            Some(DocEdge {
                from_index: index_of[&s],
                to_index: index_of[&t],
                source_port: w.source_port,
                target_port: w.target_port,
            })
        })
        .collect();

    Ok(DocModel {
        workflow_name: workflow.name.clone(),
        node_count: nodes.len(),
        edge_count: edges.len(),
        nodes,
        edges,
    })
}

// ─────────────────────────── rendering ───────────────────────────

/// Escape `& < > " '` for safe embedding in HTML text and attributes.
pub fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn render_mermaid(doc: &DocModel) -> String {
    let mut g = String::from("graph TD\n");
    for group in [NodeGroup::Source, NodeGroup::Step, NodeGroup::Destination] {
        let members: Vec<&DocNode> = doc.nodes.iter().filter(|n| n.group == group).collect();
        if members.is_empty() {
            continue;
        }
        let _ = writeln!(g, "  subgraph {}", group.label());
        for n in members {
            let _ = writeln!(
                g,
                "    n{}[\"{}\"]",
                n.topo_rank,
                escape_html(&format!("{} {}", n.topo_rank, n.type_id))
            );
        }
        let _ = writeln!(g, "  end");
    }
    for e in &doc.edges {
        let _ = writeln!(g, "  n{} -->|p{}→p{}| n{}", e.from_index, e.source_port, e.target_port, e.to_index);
    }
    g
}

fn render_css() -> String {
    [
        ":root{color-scheme:light dark}",
        "body{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;margin:2rem auto;max-width:60rem;padding:0 1rem;line-height:1.5}",
        "h1{font-size:1.4rem}h2{font-size:1.1rem;margin-top:2rem}",
        "table{border-collapse:collapse;width:100%;margin:.5rem 0 1.5rem}th,td{border:1px solid #8884;padding:.3rem .6rem;text-align:left;vertical-align:top}",
        "th{background:#8882}code,pre{font-size:.85rem}pre{overflow-x:auto;background:#8881;padding:.6rem;border-radius:.4rem}",
        ".meta{color:#888;font-size:.85rem}",
    ].join("\n")
}

/// Render the full self-contained HTML page.
pub fn render_html(doc: &DocModel, mermaid: bool) -> String {
    let mut html = String::new();
    let title = escape_html(&doc.workflow_name);
    let _ = writeln!(html, "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    let _ = writeln!(html, "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    let _ = writeln!(html, "<title>SwanFlow lineage — {title}</title>");
    let _ = writeln!(html, "<style>{}</style></head><body>", render_css());
    let _ = writeln!(html, "<h1>SwanFlow lineage — {title}</h1>");
    let _ = writeln!(
        html,
        "<p class=\"meta\">{} nodes · {} edges · generated by <code>swanflow docs</code></p>",
        doc.node_count, doc.edge_count
    );

    if mermaid {
        let _ = writeln!(html, "<h2 id=\"diagram\">Lineage diagram</h2>");
        let _ = writeln!(html, "<pre class=\"mermaid\">{}</pre>", render_mermaid(doc));
        let _ = writeln!(
            html,
            "<script type=\"module\">import(\"https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs\").then(m=>m.default.initialize({{startOnLoad:true}})).catch(()=>document.querySelector(\".mermaid\").replaceWith(\"(diagram unavailable offline — see tables below)\"));</script>"
        );
    }

    // Per-group node tables with anchor navigation.
    let _ = writeln!(html, "<h2 id=\"nodes\">Nodes</h2><p class=\"meta\">");
    for group in [NodeGroup::Source, NodeGroup::Step, NodeGroup::Destination] {
        let count = doc.nodes.iter().filter(|n| n.group == group).count();
        let _ = write!(html, "<a href=\"#g-{:?}\">{}</a> ({count}) · ", group, group.label());
    }
    let _ = writeln!(html, "</p>");

    for group in [NodeGroup::Source, NodeGroup::Step, NodeGroup::Destination] {
        let _ = writeln!(html, "<h3 id=\"g-{:?}\">{}</h3>", group, group.label());
        let _ = writeln!(html, "<table><tr><th>#</th><th>Type</th><th>State</th><th>Mode</th><th>Error mode</th><th>Config</th></tr>");
        for n in doc.nodes.iter().filter(|n| n.group == group) {
            let _ = writeln!(
                html,
                "<tr id=\"n{}\"><td>{}</td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td><pre>{}</pre></td></tr>",
                n.topo_rank, n.topo_rank, escape_html(&n.type_id), escape_html(&n.state),
                escape_html(&n.mode), escape_html(&n.error_mode), escape_html(&n.config_json)
            );
        }
        let _ = writeln!(html, "</table>");
    }

    let _ = writeln!(html, "<h2 id=\"edges\">Edges</h2>");
    let _ = writeln!(html, "<table><tr><th>From</th><th>To</th><th>Ports</th></tr>");
    for e in &doc.edges {
        let _ = writeln!(
            html,
            "<tr><td><a href=\"#n{}\">{}</a></td><td><a href=\"#n{}\">{}</a></td><td>p{} → p{}</td></tr>",
            e.from_index, escape_html(&doc.node_label(e.from_index)),
            e.to_index, escape_html(&doc.node_label(e.to_index)),
            e.source_port, e.target_port
        );
    }
    let _ = writeln!(html, "</table></body></html>");
    html
}

/// Write the rendered HTML to `path` (parent directories are created).
pub fn write_doc(path: &std::path::Path, html: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(path, html)
}

// ─────────────────────────── tests ───────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::{EdgeInstance, NodeInstance};

    fn node(type_id: &str) -> NodeInstance {
        NodeInstance {
            type_id: type_id.into(),
            ..Default::default()
        }
    }

    fn sample() -> Workflow {
        let mut wf = Workflow::default();
        wf.name = "sample".into();
        let a = wf.graph.add_node(node("core.io.duckdb_read"));
        let b = wf.graph.add_node(node("core.sql"));
        let c = wf.graph.add_node(node("core.io.duckdb_write"));
        wf.graph.add_edge(a, b, EdgeInstance { source_port: 0, target_port: 0 });
        wf.graph.add_edge(b, c, EdgeInstance { source_port: 0, target_port: 0 });
        wf
    }

    #[test]
    fn build_doc_groups_and_counts() {
        let doc = build_doc(&sample()).unwrap();
        assert_eq!(doc.node_count, 3);
        assert_eq!(doc.edge_count, 2);
        // grouping: read=Source, sql=Step, write=Destination
        let g = |t: &str| doc.nodes.iter().find(|n| n.type_id == t).unwrap().group;
        assert_eq!(g("core.io.duckdb_read"), NodeGroup::Source);
        assert_eq!(g("core.sql"), NodeGroup::Step);
        assert_eq!(g("core.io.duckdb_write"), NodeGroup::Destination);
        // topological order: read before sql before write
        let rank = |t: &str| doc.nodes.iter().find(|n| n.type_id == t).unwrap().topo_rank;
        assert!(rank("core.io.duckdb_read") < rank("core.sql"));
        assert!(rank("core.sql") < rank("core.io.duckdb_write"));
    }

    #[test]
    fn cycle_is_an_error() {
        let mut wf = Workflow::default();
        let a = wf.graph.add_node(node("a"));
        let b = wf.graph.add_node(node("b"));
        wf.graph.add_edge(a, b, EdgeInstance { source_port: 0, target_port: 0 });
        wf.graph.add_edge(b, a, EdgeInstance { source_port: 0, target_port: 0 });
        assert!(build_doc(&wf).is_err());
    }

    #[test]
    fn default_html_has_no_external_refs() {
        let doc = build_doc(&sample()).unwrap();
        let html = render_html(&doc, false);
        assert!(!html.contains("http://"));
        assert!(!html.contains("https://"));
        assert!(!html.contains("<script"));
    }

    #[test]
    fn mermaid_html_uses_cdn_and_pre_block() {
        let doc = build_doc(&sample()).unwrap();
        let html = render_html(&doc, true);
        assert!(html.contains("<pre class=\"mermaid\">"));
        assert!(html.contains("https://cdn.jsdelivr.net/npm/mermaid"));
    }

    #[test]
    fn escape_html_covers_all_five() {
        assert_eq!(escape_html("&<>\"'"), "&amp;&lt;&gt;&quot;&#39;");
        assert_eq!(escape_html("plain"), "plain");
    }

    #[test]
    fn config_with_markup_is_escaped() {
        let mut wf = sample();
        let b = wf
            .graph
            .node_weights_mut()
            .find(|n| n.type_id == "core.sql")
            .unwrap();
        b.config = serde_json::json!({"sql": "<script>alert(1)</script> & 'x'"});
        let doc = build_doc(&wf).unwrap();
        let html = render_html(&doc, false);
        assert!(!html.contains("<script>alert"));
        assert!(html.contains("&lt;script&gt;"));
    }

    #[test]
    fn write_doc_creates_parent_dirs() {
        let dir = std::env::temp_dir().join(format!("swanflow_docs_test_{}", std::process::id()));
        let path = dir.join("nested/out.html");
        write_doc(&path, "<html></html>").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "<html></html>");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

//! Self-contained single-file HTML lineage portal for a workflow.
//!
//! Rendered purely from the graph structure — no engine, no execution, no
//! schema inference. See openspec/changes/docs-html-portal/design.md.
//!
//! Default output is fully offline (no CDN). `--mermaid` adds an opt-in
//! Mermaid diagram that degrades to raw text when the CDN is unreachable.

use crate::core::graph::Workflow;
use crate::nodes::NodeState;
use petgraph::algo::toposort;
use petgraph::stable_graph::{NodeIndex, StableGraph};
use petgraph::visit::EdgeRef as _;
use petgraph::Direction;
use std::path::Path;

/// Node-count threshold above which the visual lineage view degrades to a
/// topological list + adjacency table (avoids an unwieldy CSS layout).
pub const LARGE_GRAPH_THRESHOLD: usize = 20;

/// A rendered node in the portal.
#[derive(Debug, Clone)]
pub struct NodeDoc {
    /// Unique anchor — the full node UUID. (The old first-8-char form collides:
    /// `examples/branching_flow.yaml` has three nodes sharing a `00000000`
    /// prefix, so truncated anchors merged into one.)
    pub anchor: String,
    /// Display id (short uuid prefix) — human-readable cells only.
    pub id: String,
    /// Mermaid-safe unique node id (full UUID, hyphens stripped, `n`-prefixed so
    /// it always starts with a letter). Distinct for every node, so the diagram
    /// never merges two vertices even when their 8-char prefixes collide.
    pub mermaid_id: String,
    pub type_id: String,
    pub mode: String,
    pub error_mode: String,
    pub state: String,
    pub materialize: bool,
    pub config_pretty: String,
    pub depth: usize,
    pub in_degree: usize,
    pub out_degree: usize,
    pub group: Group,
    /// Direct predecessor type_ids (empty for sources). Makes the flow visible
    /// in the lineage view without a full edge overlay.
    pub upstream: Vec<String>,
}

/// A rendered edge in the portal.
#[derive(Debug, Clone)]
pub struct EdgeDoc {
    pub source_anchor: String,
    pub source_id: String,
    pub source_type: String,
    pub target_anchor: String,
    pub target_id: String,
    pub target_type: String,
    /// Mermaid-unique endpoint ids (full-UUID based). Used ONLY for the
    /// `--mermaid` diagram so edges reference the same vertices the subgraphs
    /// declare (the display `source_id`/`target_id` are short prefixes that
    /// can collide).
    pub source_mermaid_id: String,
    pub target_mermaid_id: String,
    pub source_port: u32,
    pub target_port: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Group {
    Source,
    Step,
    Sink,
}

impl Group {
    pub fn as_str(self) -> &'static str {
        match self {
            Group::Source => "source",
            Group::Step => "step",
            Group::Sink => "sink",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Group::Source => "Sources",
            Group::Step => "Steps",
            Group::Sink => "Sinks",
        }
    }
}

/// The full model rendered into HTML.
#[derive(Debug, Clone)]
pub struct DocModel {
    pub workflow_name: String,
    pub workflow_id: String,
    pub node_count: usize,
    pub edge_count: usize,
    pub max_depth: usize,
    pub nodes: Vec<NodeDoc>,
    pub edges: Vec<EdgeDoc>,
    pub is_large: bool,
}

/// Build the render model from a workflow. Returns an error only if the
/// graph has a cycle (should already be rejected at construction).
pub fn build_doc(workflow: &Workflow) -> Result<DocModel, String> {
    let graph: &StableGraph<crate::core::graph::NodeInstance, crate::core::graph::EdgeInstance> =
        &workflow.graph;

    let order = toposort(graph, None).map_err(|_| "workflow graph has a cycle".to_string())?;
    if order.len() != graph.node_count() {
        // Isolated nodes are still in the toposort for a DAG; a count mismatch
        // here would indicate a cycle the builder missed.
        return Err("toposort did not cover all nodes (cycle?)".to_string());
    }

    // Map NodeIndex -> position in topological order (for stable anchors).
    let mut order_pos: std::collections::HashMap<NodeIndex, usize> =
        std::collections::HashMap::new();
    for (pos, idx) in order.iter().enumerate() {
        order_pos.insert(*idx, pos);
    }

    // In/out degree per node.
    let mut in_deg: std::collections::HashMap<NodeIndex, usize> = std::collections::HashMap::new();
    let mut out_deg: std::collections::HashMap<NodeIndex, usize> = std::collections::HashMap::new();
    for idx in graph.node_indices() {
        let i = graph.edges_directed(idx, Direction::Incoming).count();
        let o = graph.edges_directed(idx, Direction::Outgoing).count();
        in_deg.insert(idx, i);
        out_deg.insert(idx, o);
    }

    // Depth = longest path from any source (0 for sources).
    let mut depth: std::collections::HashMap<NodeIndex, usize> = std::collections::HashMap::new();
    for idx in &order {
        let d = graph
            .edges_directed(*idx, Direction::Incoming)
            .map(|e| depth.get(&e.source()).copied().unwrap_or(0) + 1)
            .max()
            .unwrap_or(0);
        depth.insert(*idx, d);
    }
    let max_depth = depth.values().copied().max().unwrap_or(0);

    // Anchor + doc per node, in topological order.
    let mut nodes: Vec<NodeDoc> = Vec::new();
    let mut idx_to_anchor: std::collections::HashMap<NodeIndex, String> =
        std::collections::HashMap::new();
    let mut idx_to_id: std::collections::HashMap<NodeIndex, String> =
        std::collections::HashMap::new();
    let mut idx_to_type: std::collections::HashMap<NodeIndex, String> =
        std::collections::HashMap::new();
    let mut idx_to_mermaid: std::collections::HashMap<NodeIndex, String> =
        std::collections::HashMap::new();

    for idx in &order {
        // StableGraph::node_weight returns Option; the index came straight from
        // toposort(graph), so a None here is a petgraph invariant break — report
        // it rather than unwrap-panic (keeps build_doc the module's only fallible
        // chokepoint instead of panicking mid-render).
        let n = match graph.node_weight(*idx) {
            Some(n) => n,
            None => return Err(format!(
                "toposort returned an invalid node index (petgraph invariant)"
            )),
        };
        let full_id = n.id.to_string();
        // Short prefix is display-only; the anchor and Mermaid id use the FULL
        // UUID so distinct nodes never collide (branching_flow has three nodes
        // whose first 8 chars are all "00000000").
        let short_id = full_id[..8.min(full_id.len())].to_string();
        let anchor = format!("node-{full_id}");
        // Mermaid node id: full UUID, hyphens stripped, "n" prefixed so it
        // always begins with a letter (Mermaid identifiers can't start with a
        // digit). Unique per node, so vertices never merge.
        let mermaid_id = format!("n{}", full_id.replace('-', ""));
        let in_d = *in_deg.get(idx).unwrap_or(&0);
        let out_d = *out_deg.get(idx).unwrap_or(&0);
        let group = if in_d == 0 {
            Group::Source
        } else if out_d == 0 {
            Group::Sink
        } else {
            Group::Step
        };
        idx_to_anchor.insert(*idx, anchor.clone());
        idx_to_id.insert(*idx, short_id.clone());
        idx_to_type.insert(*idx, n.type_id.clone());
        idx_to_mermaid.insert(*idx, mermaid_id.clone());
        let upstream: Vec<String> = graph
            .edges_directed(*idx, Direction::Incoming)
            .map(|e| idx_to_type.get(&e.source()).cloned().unwrap_or_default())
            .collect();
        nodes.push(NodeDoc {
            anchor,
            id: short_id,
            mermaid_id,
            type_id: n.type_id.clone(),
            mode: mode_str(&n.mode),
            error_mode: error_mode_str(&n.error_mode),
            state: state_str(&n.state),
            materialize: n.materialize,
            config_pretty: serde_json::to_string_pretty(&n.config).unwrap_or_else(|_| "{}".into()),
            depth: *depth.get(idx).unwrap_or(&0),
            in_degree: in_d,
            out_degree: out_d,
            group,
            upstream,
        });
    }

    // Edges.
    let mut edges: Vec<EdgeDoc> = Vec::new();
    for e in graph.edge_indices() {
        // edge_endpoints/edge_weight return Option; both come from our own
        // edge_indices() iteration, so a None is a petgraph invariant break.
        let (s, t) = match graph.edge_endpoints(e) {
            Some(st) => st,
            None => return Err("petgraph edge_endpoints invariant break".to_string()),
        };
        let ew = match graph.edge_weight(e) {
            Some(ew) => ew,
            None => return Err("petgraph edge_weight invariant break".to_string()),
        };
        edges.push(EdgeDoc {
            source_anchor: idx_to_anchor.get(&s).cloned().unwrap_or_default(),
            source_id: idx_to_id.get(&s).cloned().unwrap_or_default(),
            source_type: idx_to_type.get(&s).cloned().unwrap_or_default(),
            target_anchor: idx_to_anchor.get(&t).cloned().unwrap_or_default(),
            target_id: idx_to_id.get(&t).cloned().unwrap_or_default(),
            target_type: idx_to_type.get(&t).cloned().unwrap_or_default(),
            source_mermaid_id: idx_to_mermaid.get(&s).cloned().unwrap_or_default(),
            target_mermaid_id: idx_to_mermaid.get(&t).cloned().unwrap_or_default(),
            source_port: ew.source_port,
            target_port: ew.target_port,
        });
    }

    Ok(DocModel {
        workflow_name: workflow.name.clone(),
        workflow_id: workflow.id.to_string(),
        node_count: nodes.len(),
        edge_count: edges.len(),
        max_depth,
        is_large: nodes.len() > LARGE_GRAPH_THRESHOLD,
        nodes,
        edges,
    })
}

fn mode_str(m: &crate::core::graph::StepMode) -> String {
    use crate::core::graph::StepMode::*;
    match m {
        Sequential => "sequential".into(),
        FanOut => "fan_out".into(),
        Collect => "collect".into(),
        Conditional => "conditional".into(),
        Loop => "loop".into(),
    }
}

fn error_mode_str(m: &crate::core::graph::ErrorMode) -> String {
    use crate::core::graph::ErrorMode::*;
    match m {
        Fail => "fail".into(),
        Skip => "skip".into(),
        Retry(n) => format!("retry({n})"),
    }
}

fn state_str(s: &NodeState) -> String {
    use NodeState::*;
    match s {
        Unconfigured => "unconfigured".into(),
        Configured => "configured".into(),
        Pending => "pending".into(),
        Running => "running".into(),
        Waiting => "waiting".into(),
        Executed => "executed".into(),
        Error(msg) => format!("error: {msg}"),
    }
}

/// HTML-escape the five significant characters.
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

/// Render the full HTML document. `mermaid` enables the opt-in CDN diagram.
pub fn render_html(doc: &DocModel, mermaid: bool) -> String {
    let mut h = String::with_capacity(64 * 1024);
    render_head(&mut h, doc, mermaid);
    h.push_str("<body>\n");
    render_header(&mut h, doc);
    render_nav(&mut h);
    render_overview(&mut h, doc);
    render_lineage(&mut h, doc, mermaid);
    render_nodes(&mut h, doc);
    render_edges(&mut h, doc);
    render_footer(&mut h);
    h.push_str("</body></html>\n");
    h
}

fn render_head(h: &mut String, doc: &DocModel, mermaid: bool) {
    let name = escape_html(&doc.workflow_name);
    h.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
    h.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
    h.push_str(&format!("<title>{name} — SwanFlow lineage</title>\n"));
    h.push_str(STYLE);
    if mermaid {
        h.push_str(
            "<script type=\"module\">\nimport mermaid from \"https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs\";\nmermaid.initialize({startOnLoad:true,theme:\"neutral\"});\n</script>\n",
        );
    }
    h.push_str("</head>\n");
}

const STYLE: &str = r##"<style>
:root{--bg:#0e1116;--panel:#161b22;--panel2:#1c2330;--border:#2b3442;--fg:#e6edf3;--muted:#8b949e;--accent:#58a6ff;--src:#3fb950;--sink:#f778ba;--step:#58a6ff;--code:#0d1117;}
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--fg);line-height:1.5}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
.top{position:sticky;top:0;z-index:10;background:var(--panel);border-bottom:1px solid var(--border);padding:10px 20px;display:flex;gap:16px;align-items:center;flex-wrap:wrap}
.top h1{font-size:16px;margin:0;font-weight:600}
.top .meta{color:var(--muted);font-size:12px}
.nav{margin-left:auto;display:flex;gap:14px;font-size:13px}
section{padding:20px}
h2{font-size:14px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin:0 0 14px;border-bottom:1px solid var(--border);padding-bottom:8px}
.cards{display:flex;gap:12px;flex-wrap:wrap}
.card{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:14px 18px;min-width:120px}
.card .num{font-size:26px;font-weight:700;color:var(--accent)}
.card .lbl{color:var(--muted);font-size:12px;margin-top:2px}
.flow{display:grid;gap:16px;overflow-x:auto;padding:8px 0}
.col{min-width:180px;border:1px dashed var(--border);border-radius:8px;padding:10px;background:var(--panel2)}
.col h3{font-size:11px;color:var(--muted);margin:0 0 10px;text-transform:uppercase;letter-spacing:.04em}
.node{background:var(--panel);border:1px solid var(--border);border-left-width:3px;border-radius:6px;padding:8px 10px;margin-bottom:8px}
.node.source{border-left-color:var(--src)}.node.step{border-left-color:var(--step)}.node.sink{border-left-color:var(--sink)}
.node .t{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12.5px;color:var(--fg);word-break:break-all}
.node .sub{color:var(--muted);font-size:11px;margin-top:3px}
.node .up{color:var(--muted);font-size:10.5px;margin-top:4px;border-top:1px dashed var(--border);padding-top:3px}
.node .anchor{color:var(--accent);font-size:11px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--border);vertical-align:top}
th{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.04em}
tr:hover td{background:var(--panel2)}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
pre.json{background:var(--code);border:1px solid var(--border);border-radius:6px;padding:10px;overflow-x:auto;font-size:12px;color:#c9d1d9;margin:0}
.badge{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;border:1px solid var(--border)}
.b-source{color:var(--src);border-color:var(--src)}.b-step{color:var(--step);border-color:var(--step)}.b-sink{color:var(--sink);border-color:var(--sink)}
.foot{color:var(--muted);font-size:12px;padding:20px;border-top:1px solid var(--border);margin-top:20px}
.mermaid{background:var(--panel);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px;overflow-x:auto}
.hint{color:var(--muted);font-size:12px;margin-bottom:10px}
</style>"##;

fn render_header(h: &mut String, doc: &DocModel) {
    let name = escape_html(&doc.workflow_name);
    let id = escape_html(&doc.workflow_id);
    h.push_str("<header class=\"top\">\n");
    h.push_str(&format!("<h1>{name}</h1>\n"));
    h.push_str(&format!(
        "<span class=\"meta\">workflow {} · {} nodes · {} edges</span>\n",
        escape_html(&doc.workflow_id), doc.node_count, doc.edge_count
    ));
    h.push_str(
        "<nav class=\"nav\"><a href=\"#overview\">概览</a><a href=\"#lineage\">血缘</a><a href=\"#nodes\">节点</a><a href=\"#edges\">边</a></nav>\n",
    );
    let _ = &id;
    h.push_str("</header>\n");
}

fn render_nav(_h: &mut String) {}

fn render_overview(h: &mut String, doc: &DocModel) {
    let (src, step, sink) = group_counts(doc);
    h.push_str("<section id=\"overview\">\n<h2>Overview</h2>\n<div class=\"cards\">\n");
    push_card(h, doc.node_count, "nodes");
    push_card(h, doc.edge_count, "edges");
    push_card(h, doc.max_depth + 1, "depths");
    push_card(h, src, "sources");
    push_card(h, step, "steps");
    push_card(h, sink, "sinks");
    h.push_str("</div>\n");
    if doc.is_large {
        h.push_str(&format!(
            "<p class=\"hint\">Large graph ({}) — lineage shown as a topological list. Use <code>--mermaid</code> for a visual diagram.</p>\n",
            doc.node_count
        ));
    }
    h.push_str("</section>\n");
}

fn push_card(h: &mut String, num: usize, label: &str) {
    h.push_str(&format!("<div class=\"card\"><div class=\"num\">{num}</div><div class=\"lbl\">{label}</div></div>"));
}

fn group_counts(doc: &DocModel) -> (usize, usize, usize) {
    let (mut s, mut st, mut k) = (0usize, 0usize, 0usize);
    for n in &doc.nodes {
        match n.group {
            Group::Source => s += 1,
            Group::Step => st += 1,
            Group::Sink => k += 1,
        }
    }
    (s, st, k)
}

fn render_lineage(h: &mut String, doc: &DocModel, mermaid: bool) {
    h.push_str("<section id=\"lineage\">\n<h2>Lineage</h2>\n");
    if mermaid {
        render_mermaid(h, doc);
    }
    if doc.is_large {
        render_toplevelist(h, doc);
    } else {
        render_column_flow(h, doc);
    }
    h.push_str("</section>\n");
}

/// CSS grid: one column per topological depth.
fn render_column_flow(h: &mut String, doc: &DocModel) {
    let cols = doc.max_depth + 1;
    h.push_str(&format!("<div class=\"flow\" style=\"grid-template-columns:repeat({cols},minmax(180px,1fr))\">"));
    for d in 0..cols {
        let nodes: Vec<&NodeDoc> = doc.nodes.iter().filter(|n| n.depth == d).collect();
        if nodes.is_empty() {
            continue;
        }
        h.push_str("<div class=\"col\"><h3>depth </h3>");
        for n in nodes {
            h.push_str(&node_card(n));
        }
        h.push_str("</div>");
    }
    h.push_str("</div>\n");
}

fn node_card(n: &NodeDoc) -> String {
    let up = if n.upstream.is_empty() {
        String::new()
    } else {
        format!(
            "<div class=\"up\">← from <span class=\"mono\">{}</span></div>",
            escape_html(&n.upstream.join(", "))
        )
    };
    format!(
        "<a class=\"node {}\" href=\"#{anchor}\"><div class=\"t mono\">{type_id}</div><div class=\"sub\">{mode} · {err} · {state}</div>{up}<div class=\"anchor mono\">{id}</div></a>",
        n.group.as_str(),
        anchor = escape_html(&n.anchor),
        type_id = escape_html(&n.type_id),
        mode = escape_html(&n.mode),
        err = escape_html(&n.error_mode),
        state = escape_html(&n.state),
        id = escape_html(&n.id),
    )
}

/// Topological list + adjacency table for large graphs.
fn render_toplevelist(h: &mut String, doc: &DocModel) {
    h.push_str("<ol>");
    for (i, n) in doc.nodes.iter().enumerate() {
        h.push_str(&format!(
            "<li><a href=\"#{anchor}\" class=\"mono\">{type_id}</a> <span class=\"badge b-{g}\">{gl}</span> <span class=\"hint\">#{i} depth {d}</span></li>",
            anchor = escape_html(&n.anchor),
            type_id = escape_html(&n.type_id),
            g = n.group.as_str(),
            gl = n.group.label(),
            d = n.depth,
        ));
    }
    h.push_str("</ol>\n");
    // adjacency
    h.push_str("<table><thead><tr><th>#</th><th>source</th><th>target</th></tr></thead><tbody>");
    for (i, e) in doc.edges.iter().enumerate() {
        h.push_str(&format!(
            "<tr><td>{i}</td><td class=\"mono\">{s}</td><td class=\"mono\">{t}</td></tr>",
            i = i + 1,
            s = escape_html(&e.source_type),
            t = escape_html(&e.target_type),
        ));
    }
    h.push_str("</tbody></table>\n");
}

fn render_mermaid(h: &mut String, doc: &DocModel) {
    let mut m = String::new();
    m.push_str("graph LR\n");
    // subgraphs
    let src: Vec<&NodeDoc> = doc.nodes.iter().filter(|n| n.group == Group::Source).collect();
    let step: Vec<&NodeDoc> = doc.nodes.iter().filter(|n| n.group == Group::Step).collect();
    let sink: Vec<&NodeDoc> = doc.nodes.iter().filter(|n| n.group == Group::Sink).collect();
    if !src.is_empty() {
        m.push_str("  subgraph Sources\n");
        for n in &src {
            m.push_str(&format!(
                "    {}[\"{}\"]\n",
                n.mermaid_id,
                n.type_id.replace('"', "'")
            ));
        }
        m.push_str("  end\n");
    }
    if !step.is_empty() {
        m.push_str("  subgraph Steps\n");
        for n in &step {
            m.push_str(&format!(
                "    {}[\"{}\"]\n",
                n.mermaid_id,
                n.type_id.replace('"', "'")
            ));
        }
        m.push_str("  end\n");
    }
    if !sink.is_empty() {
        m.push_str("  subgraph Sinks\n");
        for n in &sink {
            m.push_str(&format!(
                "    {}[\"{}\"]\n",
                n.mermaid_id,
                n.type_id.replace('"', "'")
            ));
        }
        m.push_str("  end\n");
    }
    for e in &doc.edges {
        // Mermaid edge-label syntax is `A -->|label| B`: the label lives in the
        // pipe between the arrow and the target. (The old `A --> B |label|`
        // form is not valid flowchart grammar and would fail to render.)
        m.push_str(&format!(
            "  {} -->|p{}→p{}| {}\n",
            e.source_mermaid_id, e.source_port, e.target_port, e.target_mermaid_id
        ));
    }
    let escaped = escape_html(&m);
    h.push_str("<pre class=\"mermaid\">");
    h.push_str(&escaped);
    h.push_str("</pre>\n");
}

fn render_nodes(h: &mut String, doc: &DocModel) {
    h.push_str("<section id=\"nodes\">\n<h2>Nodes</h2>\n<table>\n<thead><tr><th>#</th><th>id</th><th>type_id</th><th>group</th><th>mode</th><th>error</th><th>state</th><th>materialize</th><th>config</th></tr></thead>\n<tbody>\n");
    for (i, n) in doc.nodes.iter().enumerate() {
        h.push_str(&format!(
            "<tr id=\"{anchor}\"><td>{i}</td><td class=\"mono\"><a href=\"#{anchor}\">{id}</a></td><td class=\"mono\">{type_id}</td><td><span class=\"badge b-{g}\">{gl}</span></td><td>{mode}</td><td>{err}</td><td>{state}</td><td>{mat}</td><td><pre class=\"json\">{cfg}</pre></td></tr>\n",
            anchor = escape_html(&n.anchor),
            i = i + 1,
            id = escape_html(&n.id),
            type_id = escape_html(&n.type_id),
            g = n.group.as_str(),
            gl = n.group.label(),
            mode = escape_html(&n.mode),
            err = escape_html(&n.error_mode),
            state = escape_html(&n.state),
            mat = n.materialize,
            cfg = escape_html(&n.config_pretty),
        ));
    }
    h.push_str("</tbody></table>\n</section>\n");
}

fn render_edges(h: &mut String, doc: &DocModel) {
    h.push_str("<section id=\"edges\">\n<h2>Edges</h2>\n<table>\n<thead><tr><th>#</th><th>source</th><th>target</th><th>port</th></tr></thead>\n<tbody>\n");
    for (i, e) in doc.edges.iter().enumerate() {
        h.push_str(&format!(
            "<tr><td>{i}</td><td class=\"mono\"><a href=\"#{s_a}\">{s_t}</a></td><td class=\"mono\"><a href=\"#{t_a}\">{t_t}</a></td><td class=\"mono\">p{s_p}→p{t_p}</td></tr>\n",
            i = i + 1,
            s_a = escape_html(&e.source_anchor),
            s_t = escape_html(&e.source_type),
            t_a = escape_html(&e.target_anchor),
            t_t = escape_html(&e.target_type),
            s_p = e.source_port,
            t_p = e.target_port,
        ));
    }
    h.push_str("</tbody></table>\n</section>\n");
}

fn render_footer(h: &mut String) {
    let ts = chrono_now_iso();
    h.push_str(&format!(
        "<footer class=\"foot\">Generated by <code>swanflow docs</code> at {ts}. Self-contained — no external resources in the default output.</footer>\n"
    ));
}

fn chrono_now_iso() -> String {
    // Best-effort UTC timestamp without a heavy dep in this module.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Format as YYYY-MM-DDTHH:MM:SSZ (civil from days).
    let (days, rem) = (secs / 86400, secs % 86400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Howard Hinnant's civil-from-days.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Write the rendered HTML to a path, creating parent dirs as needed.
pub fn write_doc(path: &Path, html: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::write(path, html)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Escape the five significant characters.
    #[test]
    fn escape_html_covers_all_five() {
        assert_eq!(escape_html("&<>\"'"), "&amp;&lt;&gt;&quot;&#39;");
        assert_eq!(escape_html("a b 123"), "a b 123");
    }

    /// A node config containing markup is escaped in the rendered output.
    #[test]
    fn config_markup_is_escaped_in_render() {
        let wf = sample_workflow();
        let doc = build_doc(&wf).unwrap();
        let html = render_html(&doc, false);
        // The config contains a raw "<b>" — it must appear escaped.
        assert!(html.contains("&lt;b&gt;"));
        assert!(!html.contains("<b>sales</b>"));
    }

    /// Default output loads no external resources: no `<script src>`, `<link>`,
    /// `<img src>`, `@import url()`, or `url(http…)` constructs. (Mermaid's CDN
    /// `<script src>` only appears under `--mermaid`.) This is the real
    /// self-contained guarantee — NOT the absence of the `http` substring, which
    /// legitimately appears in inert, escaped config text (see the next test).
    #[test]
    fn default_output_loads_no_external_resources() {
        let wf = sample_workflow();
        let doc = build_doc(&wf).unwrap();
        let html = render_html(&doc, false);
        assert!(!html.contains("<script src"));
        assert!(!html.contains("<link "));
        assert!(!html.contains("<img "));
        assert!(!html.contains("@import url"));
        assert!(!html.contains("url(http"));
    }

    /// A node config that contains a URL renders as inert, escaped text in the
    /// config cell — the page still loads no external resource, because the URL
    /// is data (inside `<pre class="json">`), not a `<script>`/`<img>`/`<link>`
    /// directive. This is what "no external resource loading" means (vs. a
    /// literal "no http substring", which a config URL would trip).
    #[test]
    fn config_url_is_inert_text_not_a_resource() {
        let mut wf = sample_workflow();
        // Point the sink node's config at a URL. Collect the sink index first
        // (immutable borrow of the graph) so the later mutation is legal.
        use petgraph::Direction as Dir;
        let sink_idx = wf
            .graph
            .node_indices()
            .find(|idx| wf.graph.edges_directed(*idx, Dir::Outgoing).count() == 0);
        if let Some(idx) = sink_idx {
            wf.graph.node_weight_mut(idx).unwrap().config =
                serde_json::json!({ "url": "https://example.com/api" });
        }
        let doc = build_doc(&wf).unwrap();
        let html = render_html(&doc, false);
        // The URL is present as escaped text in the config cell...
        assert!(html.contains("https://example.com/api"));
        // ...but the page still loads no external resource.
        assert!(!html.contains("<script src"));
        assert!(!html.contains("<img "));
        assert!(!html.contains("<link "));
        assert!(!html.contains("url(http"));
        // Config URLs are never turned into real links.
        assert!(!html.contains("<a href=\"http"));
    }

    /// Mermaid output includes a renderable diagram + CDN script.
    #[test]
    fn mermaid_output_has_diagram_and_cdn() {
        let wf = sample_workflow();
        let doc = build_doc(&wf).unwrap();
        let html = render_html(&doc, true);
        assert!(html.contains("<pre class=\"mermaid\">"));
        assert!(html.contains("graph LR"));
        assert!(html.contains("cdn.jsdelivr.net"));
    }

    /// Topological order + grouping + edge count are correct.
    #[test]
    fn sample_workflow_model_is_correct() {
        let wf = sample_workflow();
        let doc = build_doc(&wf).unwrap();
        assert_eq!(doc.node_count, 3);
        assert_eq!(doc.edge_count, 2);
        // a->b->c: depths 0,1,2 => max_depth 2.
        assert_eq!(doc.max_depth, 2);
        // First node (no incoming) is a Source; last (no outgoing) is a Sink.
        let first = &doc.nodes[0];
        let last = &doc.nodes[2];
        assert_eq!(first.group, Group::Source);
        assert_eq!(last.group, Group::Sink);
        assert_eq!(first.in_degree, 0);
        assert_eq!(last.out_degree, 0);
        // Flow visibility: source has no upstream; b (middle) flows from a.
        assert!(first.upstream.is_empty());
        assert_eq!(doc.nodes[1].upstream, vec![doc.nodes[0].type_id.clone()]);
    }

    /// Build a tiny 3-node workflow: a -> b -> c, with a config containing
    /// raw markup to exercise escaping.
    fn sample_workflow() -> Workflow {
        let mut wf = Workflow::default();
        wf.name = "sample".to_string();
        let mut a = crate::core::graph::NodeInstance::default();
        a.id = uuid::Uuid::new_v4();
        a.type_id = "core.io.duckdb_read".to_string();
        a.config = serde_json::json!({ "title": "<b>sales</b>", "query": "SELECT 1" });
        let mut b = crate::core::graph::NodeInstance::default();
        b.id = uuid::Uuid::new_v4();
        b.type_id = "core.sql".to_string();
        b.config = serde_json::json!({ "sql": "SELECT * FROM input_0" });
        let mut c = crate::core::graph::NodeInstance::default();
        c.id = uuid::Uuid::new_v4();
        c.type_id = "core.io.excel_report".to_string();
        c.config = serde_json::json!({ "path": "out.xlsx" });
        let ia = wf.graph.add_node(a);
        let ib = wf.graph.add_node(b);
        let ic = wf.graph.add_node(c);
        wf.graph.add_edge(
            ia,
            ib,
            crate::core::graph::EdgeInstance { source_port: 0, target_port: 0 },
        );
        wf.graph.add_edge(
            ib,
            ic,
            crate::core::graph::EdgeInstance { source_port: 0, target_port: 0 },
        );
        wf
    }
}

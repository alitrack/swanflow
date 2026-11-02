//! Deploy — compile a swanflow DAG into a target runtime (MotherDuck Flights).
//!
//! Openspec: 2026-11-02-dag-flights-compiler. The compiler half of this
//! change is a **pure function**: `Workflow + mode → FlightProgram` with zero
//! IO, so it is fully unit-testable without tokens or networks. The deployer
//! half (create/update via MCP) lives in `motherduck.rs` and reuses the
//! streamable HTTP transport from `crate::mcp::http`.
//!
//! Red-letter semantics inherited from the connector (docs/nodes/
//! motherduck-flight.md) and stamped into every generated program header:
//! runs-as-owner | at-least-once | version-locks at run time.

pub mod motherduck;
pub mod deployer;

use crate::core::graph::Workflow;
use petgraph::visit::EdgeRef;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Compilation output for one Flight: the Python source plus the metadata
/// the deployer needs (name, config defaults, source digest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightProgram {
    /// MotherDuck flight name (`[a-zA-Z0-9_-]`, <= 200 chars).
    pub flight_name: String,
    /// Full Python source of the flight program.
    pub source: String,
    /// sha256 of `source` — recorded in the manifest for rollback checks.
    pub source_sha256: String,
}

/// Deploy manifest — one per deployment, chained for rollback.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployManifest {
    pub dag_id: String,
    pub mode: String,
    pub deployed_at: String,
    /// Masked identity hint of the token's account (never the token).
    pub owner_hint: String,
    pub flights: Vec<FlightRecord>,
    /// Pointer (file name) of the previous manifest — chain for rollback.
    /// `None` on first deployment → rollback refused.
    pub rollback: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightRecord {
    pub flight_id: Option<String>,
    pub flight_name: String,
    pub version: u32,
    pub source_sha256: String,
}

/// The three red-letter warnings stamped into every generated program.
pub const RED_LETTERS: &str = "# ⚠️ runs as owner | at-least-once | version-locks at run time";

/// Stable table name for a node — mirrors the engine's `node_<uuid>` naming
/// (core::node_table_name) so compiled SQL and engine SQL agree.
pub fn node_table(node_id: uuid::Uuid) -> String {
    format!("node_{node_id}")
}

/// Topological order of a workflow's nodes. Deterministic: among ready nodes
/// the id-smallest is emitted first, and isolated (edge-less) nodes are
/// appended in id order after the connected ones — the compiler MUST be
/// reproducible byte-for-byte. Cycles cannot pass engine validation; if one
/// reaches us anyway the leftover nodes are appended in id order (defensive,
/// never silently dropped).
pub fn topo_order(workflow: &Workflow) -> Vec<uuid::Uuid> {
    let graph = &workflow.graph;
    let mut remaining: Vec<uuid::Uuid> = graph.node_indices().map(|i| graph[i].id).collect();
    remaining.sort();

    let preds_of = |id: uuid::Uuid| -> Vec<uuid::Uuid> {
        graph
            .node_indices()
            .find(|i| graph[*i].id == id)
            .map(|idx| {
                graph
                    .edges_directed(idx, petgraph::Direction::Incoming)
                    .map(|e| graph[e.source()].id)
                    .collect()
            })
            .unwrap_or_default()
    };

    let mut done: std::collections::BTreeSet<uuid::Uuid> = Default::default();
    let mut emitted: Vec<uuid::Uuid> = Vec::with_capacity(remaining.len());
    let mut connected_seen = false;
    while !remaining.is_empty() {
        let mut progressed = false;
        for cand in remaining.clone() {
            let preds = preds_of(cand);
            let ready = preds.iter().all(|p| done.contains(p));
            if ready {
                if !preds.is_empty() || connected_seen {
                    // part of (or downstream of) the connected flow
                }
                emitted.push(cand);
                done.insert(cand);
                remaining.retain(|id| *id != cand);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
        connected_seen = true;
    }
    // Leftovers (isolated nodes that never got a turn in the loop above, or
    // — impossibly — a cycle): append in id order.
    let mut rest = remaining;
    rest.sort();
    emitted.extend(rest);
    emitted
}

/// The whitelist — everything else is a hard compile error. `core.transform.*`
/// is accepted gate-wise but each node must still provide a static SQL
/// representation (`sql_text`), otherwise it fails compilation like any
/// non-whitelisted type.
pub fn is_compilable(type_id: &str) -> bool {
    matches!(
        type_id,
        "core.io.csv_read"
            | "core.io.parquet_read"
            | "core.io.http_fetch"
            | "core.sql"
            | "core.io.duckdb_write"
            | "connector.motherduck.flight"
    ) || type_id.starts_with("core.transform.")
}

/// Compile-time template resolution: `{{input_N}}`, `{{node_<uuid8>}}`,
/// `{{var}}`. Anything unresolved is a compile error — no runtime surprises.
fn render_template(
    input: &str,
    node_label: &str,
    input_tables: &BTreeMap<u32, String>,
    vars: &BTreeMap<String, String>,
) -> Result<String, String> {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("{{") {
        let _ = write!(out, "{}", &rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            return Err(format!("{node_label}: unclosed template brace in: {rest}"));
        };
        let key = after[..end].trim();
        let value = if let Some(idx) = key.strip_prefix("input_") {
            let port: u32 = idx.parse().map_err(|_| {
                format!("{node_label}: bad input port reference '{key}'")
            })?;
            input_tables.get(&port).cloned().ok_or_else(|| {
                format!("{node_label}: no upstream table for '{key}'")
            })?
        } else if let Some(nid) = key.strip_prefix("node_") {
            format!("node_{nid}")
        } else {
            vars.get(key).cloned().ok_or_else(|| {
                format!("{node_label}: unresolved template variable '{{{{{key}}}}}'")
            })?
        };
        let _ = write!(out, "{value}");
        rest = &after[end + 2..];
    }
    let _ = write!(out, "{rest}");
    Ok(out)
}

/// Escape a string for safe embedding in single-quoted SQL.
pub(crate) fn sql_quote(s: &str) -> String {
    s.replace('\'', "''")
}

/// Escape a string for single-quoted Python literal.
pub(crate) fn py_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn red_letters_stamp_present() {
        assert!(RED_LETTERS.contains("runs as owner"));
        assert!(RED_LETTERS.contains("at-least-once"));
    }

    fn wf3() -> Workflow {
        // a(csv) -> b(sql) -> c(duckdb_write), plus isolated d(sql)
        let mut wf = Workflow::default();
        let a = uuid::uuid!("00000000-0000-0000-0000-00000000000a");
        let b = uuid::uuid!("00000000-0000-0000-0000-00000000000b");
        let c = uuid::uuid!("00000000-0000-0000-0000-00000000000c");
        let d = uuid::uuid!("00000000-0000-0000-0000-00000000000d");
        let mk = |id: uuid::Uuid, t: &str| crate::core::graph::NodeInstance {
            id,
            type_id: t.to_string(),
            config: serde_json::json!({}),
            ..Default::default()
        };
        use petgraph::stable_graph::StableGraph;
        let mut g = StableGraph::new();
        let ia = g.add_node(mk(a, "core.io.csv_read"));
        let ib = g.add_node(mk(b, "core.sql"));
        let ic = g.add_node(mk(c, "core.io.duckdb_write"));
        g.add_edge(ia, ib, crate::core::graph::EdgeInstance { source_port: 0, target_port: 0 });
        g.add_edge(ib, ic, crate::core::graph::EdgeInstance { source_port: 0, target_port: 0 });
        let _ = g.add_node(mk(d, "core.sql")); // isolated
        wf.graph = g;
        wf
    }

    #[test]
    fn topo_order_respects_edges_and_appends_isolated() {
        let wf = wf3();
        let order = topo_order(&wf);
        let a = uuid::uuid!("00000000-0000-0000-0000-00000000000a");
        let b = uuid::uuid!("00000000-0000-0000-0000-00000000000b");
        let c = uuid::uuid!("00000000-0000-0000-0000-00000000000c");
        let d = uuid::uuid!("00000000-0000-0000-0000-00000000000d");
        let pos = |id: uuid::Uuid| order.iter().position(|x| *x == id).unwrap();
        assert!(pos(a) < pos(b));
        assert!(pos(b) < pos(c));
        assert_eq!(pos(d), 3, "isolated node appended last, in id order");
        assert_eq!(order.len(), 4);
    }

    #[test]
    fn whitelist_boundary() {
        assert!(is_compilable("core.io.csv_read"));
        assert!(is_compilable("core.transform.project"));
        assert!(is_compilable("connector.motherduck.flight"));
        assert!(!is_compilable("core.ai.llm"));
        assert!(!is_compilable("core.script.python"));
        assert!(!is_compilable("core.luajit"));
    }
}

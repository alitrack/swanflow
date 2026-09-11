//! Human-on-the-Loop (HOTL) approval gate.
//!
//! Lets a human review a (possibly agent-generated) workflow and give an
//! explicit go/no-go before it executes. The decision logic is split into
//! pure functions so it is fully unit-testable; the interactive part is a
//! thin `BufRead`/`Write` wrapper.
//!
//! Safety posture: **conservative by default**. Only an explicit affirmative
//! answers "yes"; anything else — including empty input, EOF, or an
//! unrecognized word — rejects. An unattended terminal (piped EOF) therefore
//! can never accidentally execute a workflow.

use std::io::{self, BufRead, Write};

use crate::core::graph::Workflow;

/// The outcome of a human approval decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Approval {
    Approved,
    Rejected,
}

/// Words (lower-cased) that count as an explicit affirmative.
const AFFIRMATIVE: &[&str] = &["y", "yes", "approve", "approved", "ok", "okay", "go", "1", "true"];
/// Words (lower-cased) that count as an explicit negative.
const NEGATIVE: &[&str] = &["n", "no", "reject", "rejected", "cancel", "deny", "abort", "0", "false"];

/// Render a compact, human-readable summary of a workflow for review.
///
/// Deterministic ordering: nodes are listed by graph index; edges follow the
/// graph's edge iteration order. Config values are truncated to keep the
/// review screen scannable.
pub fn render_summary(wf: &Workflow) -> String {
    let mut out = String::new();
    out.push_str(&format!("Workflow : {}\n", wf.name));
    out.push_str(&format!("Nodes    : {}\n", wf.graph.node_count()));
    out.push_str(&format!("Edges    : {}\n", wf.graph.edge_count()));
    if !wf.context.is_empty() {
        let keys: Vec<&str> = wf.context.keys().map(|k| k.as_str()).collect();
        out.push_str(&format!("Context  : {}\n", keys.join(", ")));
    }

    out.push_str("\nNodes:\n");
    for node_idx in wf.graph.node_indices() {
        let node = &wf.graph[node_idx];
        let cfg = if node.config.is_null() {
            "(no config)".to_string()
        } else {
            truncate_chars(&node.config.to_string(), 100)
        };
        out.push_str(&format!("  • {}\n", node.type_id));
        out.push_str(&format!("      config: {}\n", cfg));
    }

    out.push_str("\nEdges:\n");
    for e in wf.graph.edge_indices() {
        let Some(edge) = wf.graph.edge_weight(e) else {
            continue;
        };
        let Some((from, to)) = wf.graph.edge_endpoints(e) else {
            continue;
        };
        let from_node = &wf.graph[from];
        let to_node = &wf.graph[to];
        out.push_str(&format!(
            "  {} → {}  (port {} → {})\n",
            from_node.type_id, to_node.type_id, edge.source_port, edge.target_port
        ));
    }

    if wf.graph.node_count() == 0 {
        out.push_str("(empty workflow)\n");
    }
    out
}

/// Truncate a string to at most `max_chars` characters, appending `…` when
/// truncated. Character-based (not byte) so multibyte UTF-8 is safe.
fn truncate_chars(s: &str, max_chars: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max_chars {
        s.to_string()
    } else {
        let t: String = chars[..max_chars].iter().collect();
        format!("{}…", t)
    }
}

/// Parse a human approval answer **conservatively**.
///
/// Returns `Some(Approved)` only for an explicit affirmative;
/// `Some(Rejected)` for an explicit negative; `None` for anything else
/// (empty, whitespace, or unrecognized). Callers must treat `None` as
/// rejection — never as approval.
pub fn parse_approval(input: &str) -> Option<Approval> {
    let t = input.trim().to_lowercase();
    if t.is_empty() {
        return None;
    }
    if AFFIRMATIVE.contains(&t.as_str()) {
        Some(Approval::Approved)
    } else if NEGATIVE.contains(&t.as_str()) {
        Some(Approval::Rejected)
    } else {
        None
    }
}

/// An interactive (or scripted) approval gate.
pub struct ApprovalGate {
    /// When true, skip the prompt and approve immediately (CI / `--yes`).
    auto_approve: bool,
}

impl Default for ApprovalGate {
    fn default() -> Self {
        Self { auto_approve: false }
    }
}

impl ApprovalGate {
    /// A gate that always requires an explicit human "yes".
    pub fn new() -> Self {
        Self { auto_approve: false }
    }

    /// A gate that approves without prompting (for `--yes` / automation).
    pub fn auto_approve() -> Self {
        Self { auto_approve: true }
    }

    /// Render the workflow to `out`, prompt on `input`, and decide.
    ///
    /// Returns `true` to proceed with execution, `false` to abort. On
    /// `auto_approve` this never reads `input`. Otherwise only an explicit
    /// affirmative approves; EOF/empty/unrecognized all abort.
    pub fn review<B: BufRead, W: Write>(
        &self,
        wf: &Workflow,
        input: &mut B,
        out: &mut W,
    ) -> io::Result<bool> {
        let summary = render_summary(wf);
        writeln!(out, "{}", summary)?;

        if self.auto_approve {
            writeln!(out, "Auto-approved (non-interactive). Proceeding.")?;
            return Ok(true);
        }

        writeln!(out, "\nApprove execution? [y/N]: ")?;
        out.flush()?;

        let mut line = String::new();
        // read_line returns Ok(0) on EOF with an empty line -> conservative reject.
        let _ = input.read_line(&mut line);
        Ok(matches!(
            parse_approval(&line),
            Some(Approval::Approved)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::{NodeInstance, Workflow};
    use serde_json::json;

    /// Build a tiny deterministic workflow: a → b → c with configs on each.
    fn node(type_id: &str, config: serde_json::Value) -> NodeInstance {
        let mut n = NodeInstance::default();
        n.type_id = type_id.to_string();
        n.config = config;
        n
    }

    fn sample_workflow() -> Workflow {
        let mut wf = Workflow::new("hotl-demo");
        let a = wf.graph.add_node(node(
            "core.io.csv_read",
            json!({"path": "users.csv"}),
        ));
        let b = wf.graph.add_node(node(
            "core.transform.filter",
            json!({"expr": "age > 18"}),
        ));
        let c = wf.graph.add_node(node(
            "core.io.excel_write",
            json!({"path": "out.xlsx"}),
        ));
        wf.graph.add_edge(
            a,
            b,
            crate::core::graph::EdgeInstance { source_port: 0, target_port: 0 },
        );
        wf.graph.add_edge(
            b,
            c,
            crate::core::graph::EdgeInstance { source_port: 0, target_port: 0 },
        );
        wf
    }

    #[test]
    fn summary_shows_nodes_edges_and_config() {
        let wf = sample_workflow();
        let s = render_summary(&wf);
        assert!(s.contains("Workflow : hotl-demo"));
        assert!(s.contains("Nodes    : 3"));
        assert!(s.contains("Edges    : 2"));
        assert!(s.contains("core.io.csv_read"));
        assert!(s.contains("core.transform.filter"));
        assert!(s.contains("core.io.excel_write"));
        // Config is rendered compactly by serde_json (no spaces).
        assert!(s.contains("\"path\":\"users.csv\""));
        // An edge line connecting the node types.
        assert!(s.contains("→"));
    }

    #[test]
    fn summary_truncates_long_config() {
        let mut wf = Workflow::new("t");
        let big = json!({"blob": "x".repeat(500)});
        wf.graph.add_node(node("core.script.wasm", big));
        let s = render_summary(&wf);
        // Should be cut with an ellipsis, not the full 500-char blob.
        assert!(s.contains("…"));
        assert!(!s.contains(&"x".repeat(500)));
    }

    #[test]
    fn empty_workflow_summary_marks_empty() {
        let wf = Workflow::new("empty");
        let s = render_summary(&wf);
        assert!(s.contains("(empty workflow)"));
    }

    #[test]
    fn parse_affirmative_variants() {
        for word in ["y", "yes", "YES", "  Y  ", "approve", "ok", "go", "1", "true"] {
            assert_eq!(parse_approval(word), Some(Approval::Approved), "word: {word}");
        }
    }

    #[test]
    fn parse_negative_variants() {
        for word in ["n", "no", "NO", "reject", "cancel", "0", "false"] {
            assert_eq!(parse_approval(word), Some(Approval::Rejected), "word: {word}");
        }
    }

    #[test]
    fn parse_unrecognized_is_none_and_conserves() {
        // Unrecognized, empty, and whitespace are NOT approvals.
        assert_eq!(parse_approval(""), None);
        assert_eq!(parse_approval("   "), None);
        assert_eq!(parse_approval("maybe"), None);
        assert_eq!(parse_approval("yesplease"), None);
    }

    #[test]
    fn gate_auto_approve_skips_prompt() {
        let wf = sample_workflow();
        let mut out: Vec<u8> = Vec::new();
        // Input that would reject if it were read — proves it isn't.
        let mut input: &[u8] = b"n\n";
        let gate = ApprovalGate::auto_approve();
        assert!(gate.review(&wf, &mut input, &mut out).unwrap());
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("Auto-approved"));
    }

    #[test]
    fn gate_requires_explicit_yes() {
        let wf = sample_workflow();
        for (answer, expected) in [
            ("y\n", true),
            ("yes\n", true),
            ("n\n", false),
            ("no\n", false),
            ("maybe\n", false), // unrecognized -> reject
        ] {
            let mut out: Vec<u8> = Vec::new();
            let mut input: &[u8] = answer.as_bytes();
            let gate = ApprovalGate::new();
            assert_eq!(
                gate.review(&wf, &mut input, &mut out).unwrap(),
                expected,
                "answer: {answer}"
            );
        }
    }

    #[test]
    fn gate_rejects_on_eof() {
        // No input at all (immediate EOF) -> conservative rejection.
        let wf = sample_workflow();
        let mut out: Vec<u8> = Vec::new();
        let mut input: &[u8] = b"";
        let gate = ApprovalGate::new();
        assert!(!gate.review(&wf, &mut input, &mut out).unwrap());
    }
}

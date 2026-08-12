//! Graph repair types — GraSP-inspired typed local repair.
//!
//! Five operators (`REBIND | INSERT_PREREQ | SUBSTITUTE | REWIRE | BYPASS`)
//! upgrade LLM-driven error recovery from "edit config and re-run" to
//! graph transformations. This module defines the **types** only; the
//! graph-transformation executor `apply_repair` lives in the `swanflow`
//! crate (`swanflow::core::repair::apply_repair`), because it operates on
//! the engine's `Workflow` type (`swanflow::core::graph::Workflow`).
//!
//! Reference: GraSP — Graph-Structured Skill Compositions for LLM Agents
//! (arXiv 2604.17870, §2.4 five operators, §3.3 local-repair ablation).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Default repair neighborhood: h-hop downstream of the failed node.
pub const DEFAULT_REPAIR_HOPS: usize = 2;
/// Bounded patch size: at most 3 nodes added per repair.
pub const MAX_REPAIR_NODES: usize = 3;
/// Bounded patch size: at most 5 edge changes per repair.
pub const MAX_REPAIR_EDGES: usize = 5;

/// Node descriptor carried inside repair operators (LLM-provided).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepairNodeSpec {
    /// Optional explicit id; a fresh UUID is generated when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    pub type_id: String,
    #[serde(default)]
    pub config: Value,
}

/// Edge descriptor for REWIRE additions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepairEdgeSpec {
    /// Node id (UUID, or type_id as fallback — resolves to the most recent
    /// instance of that type).
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub source_port: u32,
    #[serde(default)]
    pub target_port: u32,
}

/// Edge reference for REWIRE removals.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EdgeRef {
    pub source: String,
    pub target: String,
}

/// Typed repair operator — corresponds to the five GraSP operators.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "operator", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RepairOperator {
    /// Modify the failed node's parameters (the original behavior).
    Rebind {
        fixed_config: Value,
    },
    /// Insert a node that establishes a missing precondition in front of the
    /// failed node.
    InsertPrereq {
        insert_node: RepairNodeSpec,
        /// Failed node id (UUID or type_id fallback).
        insert_before: String,
    },
    /// Replace the failed node's type, keeping downstream interface compatible
    /// (compatibility is checked at warning level, not hard-fail).
    Substitute {
        new_type: String,
        new_config: Value,
    },
    /// Local edge rewiring (add/remove) around the failed node's neighborhood.
    Rewire {
        add_edges: Vec<RepairEdgeSpec>,
        remove_edges: Vec<EdgeRef>,
    },
    /// Skip the failed node (current state already satisfies downstream).
    Bypass,
}

/// LLM response wrapper — accepts both the tagged operator format and the
/// legacy bare `{"fixed_config": ...}` shape (backward compatible).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum RepairResponse {
    Tagged(RepairOperator),
    LegacyRebind {
        fixed_config: Value,
    },
}

impl RepairResponse {
    pub fn into_operator(self) -> RepairOperator {
        match self {
            RepairResponse::Tagged(op) => op,
            RepairResponse::LegacyRebind { fixed_config } => {
                RepairOperator::Rebind { fixed_config }
            }
        }
    }
}

/// Result of applying a repair to a workflow (produced by
/// `swanflow::core::repair::apply_repair`).
#[derive(Debug, Clone)]
pub struct RepairOutcome {
    pub applied: RepairOperator,
    /// Node ids (UUID strings) whose cached output is invalidated — these are
    /// the nodes that must be re-executed after the repair.
    pub affected_nodes: Vec<String>,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serde_rebind_roundtrip() {
        let op = RepairOperator::Rebind {
            fixed_config: json!({"path": "/fix.csv"}),
        };
        let s = serde_json::to_string(&op).unwrap();
        assert_eq!(
            s,
            r#"{"operator":"REBIND","fixed_config":{"path":"/fix.csv"}}"#
        );
        let back: RepairOperator = serde_json::from_str(&s).unwrap();
        assert_eq!(back, op);
    }

    #[test]
    fn serde_all_five_operators_roundtrip() {
        let ops = vec![
            RepairOperator::Rebind {
                fixed_config: json!({"a": 1}),
            },
            RepairOperator::InsertPrereq {
                insert_node: RepairNodeSpec {
                    id: None,
                    type_id: "core.sql".into(),
                    config: json!({"query": "SELECT 1"}),
                },
                insert_before: "abc-123".into(),
            },
            RepairOperator::Substitute {
                new_type: "core.sql".into(),
                new_config: json!({}),
            },
            RepairOperator::Rewire {
                add_edges: vec![RepairEdgeSpec {
                    source: "a".into(),
                    target: "b".into(),
                    source_port: 0,
                    target_port: 0,
                }],
                remove_edges: vec![EdgeRef {
                    source: "c".into(),
                    target: "d".into(),
                }],
            },
            RepairOperator::Bypass,
        ];
        for op in ops {
            let s = serde_json::to_string(&op).unwrap();
            let back: RepairOperator = serde_json::from_str(&s).unwrap();
            assert_eq!(back, op, "roundtrip failed for {s}");
        }
    }

    #[test]
    fn legacy_rebind_format_parses() {
        // Bare {"fixed_config": ...} without operator tag → Rebind.
        let resp: RepairResponse =
            serde_json::from_str(r#"{"fixed_config":{"path":"/x"}}"#).unwrap();
        let op = resp.into_operator();
        match op {
            RepairOperator::Rebind { fixed_config } => assert_eq!(fixed_config["path"], "/x"),
            other => panic!("expected Rebind, got {:?}", other),
        }
    }

    #[test]
    fn tagged_response_parses() {
        let resp: RepairResponse =
            serde_json::from_str(r#"{"operator":"BYPASS"}"#).unwrap();
        assert_eq!(resp.into_operator(), RepairOperator::Bypass);
    }
}

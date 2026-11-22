//! Graph repair executor — applies GraSP repair operators to the engine's
//! [`Workflow`] (the `swanflow::core::graph::Workflow` instance used by
//! [`SwanFlowEngine`](crate::core::engine::SwanFlowEngine)).
//!
//! Operator *types* live in `swanflow_core::repair` (shared with swanflow-ai);
//! this module implements the graph transformations against the engine's
//! local `Workflow`, which carries a parallel type system to `swanflow-core`.

use crate::core::graph::{EdgeInstance, NodeInstance, Workflow};
use crate::nodes::NodeState;
use anyhow::{anyhow, Result};
use swanflow_core::repair::{RepairOperator, RepairOutcome, DEFAULT_REPAIR_HOPS, MAX_REPAIR_EDGES};
use uuid::Uuid;

/// Resolve a node reference: UUID first, then most-recent type_id match.
fn resolve_node_ref(
    workflow: &Workflow,
    reference: &str,
) -> Option<petgraph::stable_graph::NodeIndex> {
    if let Ok(uuid) = Uuid::parse_str(reference) {
        return workflow.find_node_by_id(&uuid);
    }
    // type_id fallback: most recent instance (reverse iteration).
    workflow.graph.node_indices().rev().find(|&idx| {
        workflow
            .graph
            .node_weight(idx)
            .map(|n| n.type_id == reference)
            .unwrap_or(false)
    })
}

/// Apply a repair operator to a workflow.
///
/// The failed node is identified by `failed_node_id` (its UUID). Every
/// operator validates DAG validity; on failure the caller escalates to the
/// existing human-in-the-loop path.
pub fn apply_repair(
    workflow: &mut Workflow,
    failed_node_id: &Uuid,
    op: RepairOperator,
) -> Result<RepairOutcome> {
    let failed_idx = workflow
        .find_node_by_id(failed_node_id)
        .ok_or_else(|| anyhow!("failed node {failed_node_id} not found in workflow"))?;

    match &op {
        RepairOperator::Rebind { fixed_config } => {
            workflow.update_config(failed_idx, fixed_config.clone())?;
            Ok(RepairOutcome {
                applied: op.clone(),
                affected_nodes: vec![failed_node_id.to_string()],
                message: format!("REBIND: updated config of node {failed_node_id}"),
            })
        }

        RepairOperator::InsertPrereq {
            insert_node,
            insert_before,
        } => {
            let target_idx = resolve_node_ref(workflow, insert_before).ok_or_else(|| {
                anyhow!("INSERT_PREREQ: insert_before node '{insert_before}' not found")
            })?;
            // Reject duplicate explicit id.
            if let Some(id) = insert_node.id {
                if workflow.find_node_by_id(&id).is_some() {
                    return Err(anyhow!(
                        "INSERT_PREREQ: node id {id} already exists in workflow"
                    ));
                }
            }
            let node = NodeInstance::new(
                insert_node.id.unwrap_or_else(Uuid::new_v4),
                insert_node.type_id.clone(),
                insert_node.config.clone(),
                NodeState::Configured,
            );
            let new_idx = workflow.add_node(node);
            // Wire new → target (cycle check inside add_edge).
            workflow.add_edge(
                new_idx,
                target_idx,
                EdgeInstance {
                    source_port: 0,
                    target_port: 0,
                },
            )?;
            let new_id = workflow.get_node(new_idx).expect("just added").id;
            Ok(RepairOutcome {
                applied: op.clone(),
                affected_nodes: vec![new_id.to_string(), failed_node_id.to_string()],
                message: format!("INSERT_PREREQ: inserted node {new_id} before {insert_before}"),
            })
        }

        RepairOperator::Substitute {
            new_type,
            new_config,
        } => {
            let downstream = workflow.downstream_of(failed_idx);
            let node = workflow
                .graph
                .node_weight_mut(failed_idx)
                .expect("failed node exists");
            node.type_id = new_type.clone();
            node.config = new_config.clone();
            node.state = NodeState::Unconfigured;
            // Downstream interface compatibility — warning level, not hard fail
            // (schema drift is caught by downstream validation at re-run).
            if !downstream.is_empty() {
                eprintln!(
                    "[repair] SUBSTITUTE: node {failed_node_id} type -> {new_type}; \
                     {} downstream edge(s) may need schema re-validation",
                    downstream.len()
                );
            }
            let mut affected = vec![failed_node_id.to_string()];
            affected.extend(
                workflow
                    .downstream_up_to_h(failed_idx, DEFAULT_REPAIR_HOPS)
                    .into_iter()
                    .filter_map(|idx| workflow.graph.node_weight(idx).map(|n| n.id.to_string())),
            );
            Ok(RepairOutcome {
                applied: op.clone(),
                affected_nodes: affected,
                message: format!(
                    "SUBSTITUTE: node {failed_node_id} type -> {new_type}, config updated"
                ),
            })
        }

        RepairOperator::Rewire {
            add_edges,
            remove_edges,
        } => {
            if add_edges.len() + remove_edges.len() > MAX_REPAIR_EDGES {
                return Err(anyhow!(
                    "REWIRE: {} edge changes exceeds bound |ΔE|≤{MAX_REPAIR_EDGES}",
                    add_edges.len() + remove_edges.len()
                ));
            }
            let mut touched: Vec<Uuid> = Vec::new();
            let record = |workflow: &Workflow,
                          idx: petgraph::stable_graph::NodeIndex,
                          touched: &mut Vec<Uuid>| {
                if let Some(id) = workflow.graph.node_weight(idx).map(|n| n.id) {
                    if !touched.contains(&id) {
                        touched.push(id);
                    }
                }
            };
            // Removals first (idempotent: missing edges are skipped).
            for e in remove_edges {
                if let (Some(s), Some(t)) = (
                    resolve_node_ref(workflow, &e.source),
                    resolve_node_ref(workflow, &e.target),
                ) {
                    let _ = workflow.remove_edge(s, t);
                    record(workflow, s, &mut touched);
                    record(workflow, t, &mut touched);
                }
            }
            // Additions (cycle check inside add_edge → reject invalid repair).
            for e in add_edges {
                let s = resolve_node_ref(workflow, &e.source)
                    .ok_or_else(|| anyhow!("REWIRE: source '{}' not found", e.source))?;
                let t = resolve_node_ref(workflow, &e.target)
                    .ok_or_else(|| anyhow!("REWIRE: target '{}' not found", e.target))?;
                workflow.add_edge(
                    s,
                    t,
                    EdgeInstance {
                        source_port: e.source_port,
                        target_port: e.target_port,
                    },
                )?;
                record(workflow, s, &mut touched);
                record(workflow, t, &mut touched);
            }
            let affected: Vec<String> = touched.iter().map(|u| u.to_string()).collect();
            Ok(RepairOutcome {
                applied: op.clone(),
                affected_nodes: affected,
                message: format!(
                    "REWIRE: removed {} edge(s), added {} edge(s)",
                    remove_edges.len(),
                    add_edges.len()
                ),
            })
        }

        RepairOperator::Bypass => {
            // Short-circuit: remove the failed node and wire its upstream
            // directly to its downstream (a true graph bypass — the node's
            // current state is assumed to already satisfy downstream needs).
            let upstream = workflow.upstream_of(failed_idx);
            let downstream = workflow.downstream_of(failed_idx);
            for u in &upstream {
                for d in &downstream {
                    // Cycle check inside add_edge; u→d cannot cycle in a DAG
                    // that previously had u → failed → d.
                    workflow.add_edge(
                        *u,
                        *d,
                        EdgeInstance {
                            source_port: 0,
                            target_port: 0,
                        },
                    )?;
                }
            }
            workflow.remove_node(failed_idx);
            let mut affected = vec![failed_node_id.to_string()];
            affected.extend(
                downstream
                    .iter()
                    .filter_map(|&i| workflow.get_node(i).map(|n| n.id.to_string())),
            );
            Ok(RepairOutcome {
                applied: op.clone(),
                affected_nodes: affected,
                message: format!(
                    "BYPASS: node {failed_node_id} removed (short-circuited); downstream continues"
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swanflow_core::repair::{EdgeRef, RepairEdgeSpec, RepairNodeSpec};
    use uuid::Uuid;

    fn build_chain(n: usize) -> (Workflow, Vec<Uuid>) {
        let mut wf = Workflow::new("chain");
        let mut ids = Vec::new();
        for i in 0..n {
            let id = Uuid::new_v4();
            ids.push(id);
            wf.add_node(NodeInstance::new(
                id,
                format!("node.{i}"),
                serde_json::json!({"i": i}),
                NodeState::Configured,
            ));
        }
        for i in 0..n - 1 {
            wf.add_edge(
                wf.find_node_by_id(&ids[i]).unwrap(),
                wf.find_node_by_id(&ids[i + 1]).unwrap(),
                EdgeInstance {
                    source_port: 0,
                    target_port: 0,
                },
            )
            .unwrap();
        }
        (wf, ids)
    }

    #[test]
    fn rebind_updates_config_and_invalidates_only_failed_node() {
        let (mut wf, ids) = build_chain(3);
        let failed = ids[1];
        let outcome = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::Rebind {
                fixed_config: serde_json::json!({"fixed": true}),
            },
        )
        .unwrap();
        assert_eq!(outcome.affected_nodes, vec![failed.to_string()]);
        let node = wf
            .find_node_by_id(&failed)
            .and_then(|i| wf.get_node(i))
            .unwrap();
        assert_eq!(node.config["fixed"], true);
        assert_eq!(node.state, NodeState::Unconfigured);
        assert_eq!(wf.graph.node_count(), 3);
        assert!(!petgraph::algo::is_cyclic_directed(&wf.graph));
    }

    #[test]
    fn insert_prereq_adds_node_and_edge() {
        let (mut wf, ids) = build_chain(2);
        let failed = ids[1];
        let outcome = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::InsertPrereq {
                insert_node: RepairNodeSpec {
                    id: None,
                    type_id: "core.sql".into(),
                    config: serde_json::json!({"query": "CREATE TABLE t AS SELECT 1 AS x"}),
                },
                insert_before: failed.to_string(),
            },
        )
        .unwrap();
        assert_eq!(outcome.affected_nodes.len(), 2);
        assert_eq!(wf.graph.node_count(), 3);
        let failed_idx = wf.find_node_by_id(&failed).unwrap();
        let upstream = wf.upstream_of(failed_idx);
        assert_eq!(upstream.len(), 2, "original upstream + new prereq");
        assert!(!petgraph::algo::is_cyclic_directed(&wf.graph));
        let new_ids: Vec<String> = upstream
            .iter()
            .filter_map(|&i| wf.get_node(i).map(|n| n.id.to_string()))
            .collect();
        for a in &outcome.affected_nodes {
            assert!(new_ids.contains(a) || *a == failed.to_string());
        }
    }

    #[test]
    fn insert_prereq_rejects_duplicate_id() {
        let (mut wf, ids) = build_chain(2);
        let failed = ids[1];
        let err = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::InsertPrereq {
                insert_node: RepairNodeSpec {
                    id: Some(failed),
                    type_id: "core.sql".into(),
                    config: serde_json::json!({}),
                },
                insert_before: failed.to_string(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn substitute_changes_type_and_invalidates_downstream() {
        let (mut wf, ids) = build_chain(3);
        let failed = ids[1];
        let outcome = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::Substitute {
                new_type: "core.script.python".into(),
                new_config: serde_json::json!({"code": "return 1"}),
            },
        )
        .unwrap();
        assert!(outcome.affected_nodes.len() >= 2);
        let node = wf
            .find_node_by_id(&failed)
            .and_then(|i| wf.get_node(i))
            .unwrap();
        assert_eq!(node.type_id, "core.script.python");
        assert_eq!(node.config["code"], "return 1");
        assert_eq!(wf.graph.node_count(), 3);
        assert!(!petgraph::algo::is_cyclic_directed(&wf.graph));
    }

    #[test]
    fn rewire_adds_and_removes_edges_within_bound() {
        let (mut wf, ids) = build_chain(3);
        let failed = ids[1];
        let outcome = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::Rewire {
                add_edges: vec![RepairEdgeSpec {
                    source: ids[0].to_string(),
                    target: ids[2].to_string(),
                    source_port: 0,
                    target_port: 0,
                }],
                remove_edges: vec![EdgeRef {
                    source: ids[0].to_string(),
                    target: ids[1].to_string(),
                }],
            },
        )
        .unwrap();
        assert!(outcome
            .message
            .contains("removed 1 edge(s), added 1 edge(s)"));
        let n0 = wf.find_node_by_id(&ids[0]).unwrap();
        let n1 = wf.find_node_by_id(&ids[1]).unwrap();
        let n2 = wf.find_node_by_id(&ids[2]).unwrap();
        assert!(wf.graph.find_edge(n0, n1).is_none());
        assert!(wf.graph.find_edge(n0, n2).is_some());
        assert!(!petgraph::algo::is_cyclic_directed(&wf.graph));
    }

    #[test]
    fn rewire_rejects_cycle() {
        let (mut wf, ids) = build_chain(3);
        let failed = ids[1];
        let err = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::Rewire {
                add_edges: vec![RepairEdgeSpec {
                    source: ids[2].to_string(),
                    target: ids[0].to_string(),
                    source_port: 0,
                    target_port: 0,
                }],
                remove_edges: vec![],
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("cycle"));
        assert_eq!(wf.graph.edge_count(), 2);
    }

    #[test]
    fn rewire_rejects_over_bound() {
        let (mut wf, ids) = build_chain(3);
        let failed = ids[1];
        let edges: Vec<RepairEdgeSpec> = (0..6)
            .map(|_i| RepairEdgeSpec {
                source: ids[0].to_string(),
                target: ids[2].to_string(),
                source_port: 0,
                target_port: 0,
            })
            .collect();
        let err = apply_repair(
            &mut wf,
            &failed,
            RepairOperator::Rewire {
                add_edges: edges,
                remove_edges: vec![],
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("|ΔE|≤5"));
    }

    #[test]
    fn bypass_short_circuits_and_invalidates_downstream() {
        let (mut wf, ids) = build_chain(3);
        let failed = ids[1];
        let outcome = apply_repair(&mut wf, &failed, RepairOperator::Bypass).unwrap();
        // Failed node removed; upstream now directly feeds downstream.
        assert!(wf.find_node_by_id(&failed).is_none(), "failed node removed");
        assert_eq!(wf.graph.node_count(), 2);
        let n0 = wf.find_node_by_id(&ids[0]).unwrap();
        let n2 = wf.find_node_by_id(&ids[2]).unwrap();
        assert!(
            wf.graph.find_edge(n0, n2).is_some(),
            "0 → 2 short-circuit edge"
        );
        assert!(!petgraph::algo::is_cyclic_directed(&wf.graph));
        // affected = failed + direct downstream (node 2).
        assert!(outcome.affected_nodes.contains(&failed.to_string()));
        assert!(outcome.affected_nodes.contains(&ids[2].to_string()));
    }

    #[test]
    fn failed_node_missing_rejected() {
        let (mut wf, _ids) = build_chain(2);
        let ghost = Uuid::new_v4();
        let err = apply_repair(&mut wf, &ghost, RepairOperator::Bypass).unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn type_id_fallback_resolution() {
        let (wf, ids) = build_chain(2);
        let idx = resolve_node_ref(&wf, "node.1");
        assert_eq!(idx, wf.find_node_by_id(&ids[1]));
        assert!(resolve_node_ref(&wf, "node.99").is_none());
    }
}

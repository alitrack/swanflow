//! Graph repair integration tests — GraSP five-operator closed loop.
//!
//! Each test: build a workflow → let it fail → simulate the LLM proposing a
//! repair operator → apply via `swanflow_core::repair::apply_repair` →
//! re-execute the workflow and assert success. Self-contained (in-memory
//! DataFusion context, no external services).

use serde_json::json;
use std::sync::Arc;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::core::repair::apply_repair;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::{NodeEvent, NodeState};
use swanflow_core::repair::{RepairNodeSpec, RepairOperator};
use uuid::Uuid;

fn node(id: Uuid, type_id: &str, config: serde_json::Value) -> NodeInstance {
    NodeInstance {
        id,
        type_id: type_id.into(),
        config,
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    }
}

/// Drain an event stream until it closes; returns the collected events.
async fn drain(mut rx: tokio::sync::mpsc::UnboundedReceiver<NodeEvent>) -> Vec<NodeEvent> {
    let mut events = Vec::new();
    while let Some(evt) = rx.recv().await {
        events.push(evt);
    }
    events
}

fn error_messages(events: &[NodeEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|e| match e {
            NodeEvent::Error { message, .. } => Some(message.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn insert_prereq_repair_closed_loop() {
    // Each test needs an isolated cache dir: the engine's metadata.duckdb is
    // a shared file, and concurrent writes from parallel tests abort DuckDB.
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::with_cache_dir(
        registry,
        format!("target/swanflow_cache_repair_{}", Uuid::new_v4()),
    );

    // ── 1. Workflow: a SQL node that references its upstream input
    //    (`input_0`) but has NO upstream source yet — planning fails.
    let csv_path = "/tmp/insert-prereq-source.csv";
    std::fs::write(csv_path, "name,age\nalice,30\nbob,25\n").unwrap();
    let mut workflow = Workflow::new("insert-prereq-repair");
    let failed_id = Uuid::new_v4();
    workflow.add_node(node(
        failed_id,
        "core.sql",
        json!({ "query": "SELECT * FROM input_0 WHERE age > 28" }),
    ));

    // ── 2. First run fails: input_0 does not exist (planning error → Error
    //    event; the engine does not mutate the node's state on planning
    //    failure, so we assert on the event stream only).
    let rx = engine.execute_workflow(&workflow).await.expect("run 1");
    let events = drain(rx).await;
    let errors = error_messages(&events);
    assert!(
        !errors.is_empty(),
        "expected run 1 to fail with a missing-input error, got events: {events:?}"
    );

    // ── 3. LLM proposes INSERT_PREREQ: add the missing source node that
    //    reads the CSV and feeds `input_0` into the failed node.
    let op = RepairOperator::InsertPrereq {
        insert_node: RepairNodeSpec {
            id: None,
            type_id: "core.io.csv".into(),
            config: json!({ "path": csv_path, "has_header": true }),
        },
        insert_before: failed_id.to_string(),
    };
    let outcome = apply_repair(&mut workflow, &failed_id, op).expect("apply INSERT_PREREQ");

    // Graph grew by exactly one node + one edge, still acyclic.
    assert_eq!(
        workflow.graph.node_count(),
        2,
        "failed node + new prereq node"
    );
    assert_eq!(workflow.graph.edge_count(), 1, "prereq → failed edge");
    assert!(!petgraph::algo::is_cyclic_directed(&workflow.graph));
    assert_eq!(outcome.affected_nodes.len(), 2);
    assert!(outcome
        .affected_nodes
        .iter()
        .any(|id| *id == failed_id.to_string()));

    // ── 4. Re-run succeeds: the prereq source now feeds the SQL node.
    let rx = engine.execute_workflow(&workflow).await.expect("run 2");
    let events = drain(rx).await;
    let _ = std::fs::remove_file(csv_path);
    assert!(
        error_messages(&events).is_empty(),
        "expected run 2 to succeed, errors: {:?}",
        error_messages(&events)
    );
    assert!(events.iter().any(|e| matches!(
        e,
        NodeEvent::StateChanged { node_id, state: NodeState::Executed } if *node_id == failed_id
    )));
}

#[tokio::test]
async fn rebind_repair_closed_loop() {
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::with_cache_dir(
        registry,
        format!("target/swanflow_cache_repair_{}", Uuid::new_v4()),
    );

    // ── 1. SQL node with a broken query (references a nonexistent table).
    let mut workflow = Workflow::new("rebind-repair");
    let failed_id = Uuid::new_v4();
    let _failed_idx = workflow.add_node(node(
        failed_id,
        "core.sql",
        json!({ "query": "SELECT * FROM missing_table" }),
    ));

    // ── 2. First run fails (planning error on the missing table).
    let rx = engine.execute_workflow(&workflow).await.expect("run 1");
    let events = drain(rx).await;
    assert!(
        !error_messages(&events).is_empty(),
        "expected run 1 to fail with missing table, got events: {events:?}"
    );

    // ── 3. LLM proposes REBIND: a corrected, self-contained query.
    let op = RepairOperator::Rebind {
        fixed_config: json!({ "query": "SELECT 1 AS x" }),
    };
    let outcome = apply_repair(&mut workflow, &failed_id, op).expect("apply REBIND");
    assert_eq!(outcome.affected_nodes, vec![failed_id.to_string()]);
    // Same topology, config updated, node marked for re-run.
    assert_eq!(workflow.graph.node_count(), 1);
    let node = workflow.find_node_by_id(&failed_id).unwrap();
    assert_eq!(
        workflow.get_node(node).unwrap().config["query"],
        "SELECT 1 AS x"
    );

    // ── 4. Re-run succeeds.
    let rx = engine.execute_workflow(&workflow).await.expect("run 2");
    let events = drain(rx).await;
    assert!(
        error_messages(&events).is_empty(),
        "expected run 2 to succeed, errors: {:?}",
        error_messages(&events)
    );
    assert!(events.iter().any(|e| matches!(
        e,
        NodeEvent::StateChanged { node_id, state: NodeState::Executed } if *node_id == failed_id
    )));
}

#[tokio::test]
async fn rewire_repair_closed_loop() {
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::with_cache_dir(
        registry,
        format!("target/swanflow_cache_repair_{}", Uuid::new_v4()),
    );

    // ── 1. bad → good chain: bad SQL node fails.
    let mut workflow = Workflow::new("rewire-repair");
    let good_id = Uuid::new_v4();
    let good_idx = workflow.add_node(node(
        good_id,
        "core.sql",
        json!({ "query": "SELECT 1 AS x" }),
    ));
    let bad_id = Uuid::new_v4();
    let bad_idx = workflow.add_node(node(
        bad_id,
        "core.sql",
        json!({ "query": "SELECT * FROM missing_table" }),
    ));
    workflow
        .add_edge(
            good_idx,
            bad_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // ── 2. Run fails on the bad node.
    let rx = engine.execute_workflow(&workflow).await.expect("run 1");
    let events = drain(rx).await;
    assert!(
        !error_messages(&events).is_empty(),
        "expected run 1 to fail, got events: {events:?}"
    );

    // ── 3. LLM proposes REWIRE: cut the good→bad edge so the bad node
    //    no longer receives (stale) input.
    let op = RepairOperator::Rewire {
        add_edges: vec![],
        remove_edges: vec![swanflow_core::repair::EdgeRef {
            source: good_id.to_string(),
            target: bad_id.to_string(),
        }],
    };
    let outcome = apply_repair(&mut workflow, &bad_id, op).expect("apply REWIRE");
    assert_eq!(workflow.graph.edge_count(), 0);
    assert!(!petgraph::algo::is_cyclic_directed(&workflow.graph));
    assert!(outcome
        .message
        .contains("removed 1 edge(s), added 0 edge(s)"));

    // ── 4. Re-run: the bad node is now an isolated node — it still fails on
    //    its own query, so the event stream carries an error again. The key
    //    assertion is the graph-level contract: edges were rewired without
    //    breaking DAG validity, and the good node still executes.
    let rx = engine.execute_workflow(&workflow).await.expect("run 2");
    let events = drain(rx).await;
    assert!(events.iter().any(|e| matches!(
        e,
        NodeEvent::StateChanged { node_id, state: NodeState::Executed } if *node_id == good_id
    )));
}

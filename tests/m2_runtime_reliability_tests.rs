use swanflow::nodes::ai::runtime::{
    AgentLoopOrchestratorNode, CheckpointResumeControllerNode, ObservabilityAuditLedgerNode,
    RetryBackoffGuardNode, RunStateStoreNode,
};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeBehavior;
use serde_json::json;

#[test]
fn test_retry_backoff_guard_deduplicates_idempotency_key_and_delays() {
    let node = RetryBackoffGuardNode;
    let mut seen = std::collections::HashSet::new();

    let first = node
        .plan_retry(
            "run-1",
            "step-a",
            &mut seen,
            &json!({ "max_retries": 3, "base_delay_ms": 50 }),
        )
        .expect("first run should be accepted");
    assert_eq!(first.delays_ms, vec![50, 100, 200]);
    assert!(!first.duplicate);

    let second = node
        .plan_retry(
            "run-1",
            "step-a",
            &mut seen,
            &json!({ "max_retries": 3, "base_delay_ms": 50 }),
        )
        .expect("duplicate idempotency key should be handled");
    assert!(second.duplicate);
    assert!(second.delays_ms.is_empty());
}

#[test]
fn test_checkpoint_resume_controller_roundtrip() {
    let mut node = CheckpointResumeControllerNode::default();
    let snapshot = json!({ "cursor": 12, "status": "running" });

    let token = node
        .save_checkpoint("run-77", snapshot.clone())
        .expect("save checkpoint should succeed");
    let resumed = node
        .resume_checkpoint(&token)
        .expect("resume checkpoint should recover snapshot");

    assert_eq!(resumed, snapshot);
}

#[test]
fn test_run_state_store_put_get_delete() {
    let mut node = RunStateStoreNode::default();
    node.put_state("run-2", "k", json!("v")).expect("put state");
    let got = node.get_state("run-2", "k").expect("get state");
    assert_eq!(got, Some(json!("v")));

    node.delete_state("run-2", "k").expect("delete state");
    let got = node.get_state("run-2", "k").expect("get deleted state");
    assert_eq!(got, None);
}

#[test]
fn test_audit_ledger_records_and_filters_events() {
    let mut node = ObservabilityAuditLedgerNode::default();

    node.record_event("run-3", "info", "started")
        .expect("record info");
    node.record_event("run-3", "error", "failed once")
        .expect("record error");
    node.record_event("run-9", "info", "other")
        .expect("record other");

    let run3 = node.query_events(Some("run-3"), None);
    assert_eq!(run3.len(), 2);

    let errors = node.query_events(None, Some("error"));
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].message, "failed once");
}

#[test]
fn test_agent_loop_orchestrator_stops_at_max_iterations() {
    let node = AgentLoopOrchestratorNode;

    let report = node
        .orchestrate("research", &json!({ "max_iterations": 3 }), |iteration| {
            format!("step-{}", iteration)
        })
        .expect("orchestration should succeed");

    assert_eq!(report.iterations, 3);
    assert_eq!(report.steps, vec!["step-1", "step-2", "step-3"]);
    assert!(report.completed);
}

#[test]
fn test_m2_runtime_nodes_reject_invalid_config() {
    let retry = RetryBackoffGuardNode;
    let err = retry
        .validate(&json!({ "max_retries": 0 }))
        .expect_err("max_retries=0 should fail");
    assert!(err.to_string().contains("max_retries"));

    let checkpoint = CheckpointResumeControllerNode::default();
    let err = checkpoint
        .validate(&json!({ "token_ttl_seconds": 0 }))
        .expect_err("token ttl=0 should fail");
    assert!(err.to_string().contains("token_ttl_seconds"));

    let state = RunStateStoreNode::default();
    let err = state
        .validate(&json!({ "max_entries_per_run": 0 }))
        .expect_err("max entries=0 should fail");
    assert!(err.to_string().contains("max_entries_per_run"));

    let audit = ObservabilityAuditLedgerNode::default();
    let err = audit
        .validate(&json!({ "max_events": 0 }))
        .expect_err("max events=0 should fail");
    assert!(err.to_string().contains("max_events"));

    let orchestrator = AgentLoopOrchestratorNode;
    let err = orchestrator
        .validate(&json!({ "max_iterations": 0 }))
        .expect_err("max_iterations=0 should fail");
    assert!(err.to_string().contains("max_iterations"));
}

#[test]
fn test_m2_runtime_nodes_are_registered() {
    let registry = NodeRegistry::default();

    assert_eq!(
        registry
            .get_node("core.ai.retry_backoff_idempotency_guard")
            .expect("retry guard should be registered")
            .spec()
            .type_id,
        "core.ai.retry_backoff_idempotency_guard"
    );
    assert_eq!(
        registry
            .get_node("core.ai.checkpoint_resume_controller")
            .expect("checkpoint controller should be registered")
            .spec()
            .type_id,
        "core.ai.checkpoint_resume_controller"
    );
    assert_eq!(
        registry
            .get_node("core.ai.run_state_store")
            .expect("run state store should be registered")
            .spec()
            .type_id,
        "core.ai.run_state_store"
    );
    assert_eq!(
        registry
            .get_node("core.ai.observability_audit_ledger")
            .expect("audit ledger should be registered")
            .spec()
            .type_id,
        "core.ai.observability_audit_ledger"
    );
    assert_eq!(
        registry
            .get_node("core.ai.agent_loop_orchestrator")
            .expect("agent loop orchestrator should be registered")
            .spec()
            .type_id,
        "core.ai.agent_loop_orchestrator"
    );
}

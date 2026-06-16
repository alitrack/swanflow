use serde_json::json;
use swanflow::nodes::ai::privacy::PolicyPrivacyGateNode;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeBehavior;

#[test]
fn test_policy_gate_denies_when_pii_present() {
    let node = PolicyPrivacyGateNode;

    let decision = node
        .evaluate_policy(
            "external_send",
            true,
            &json!({
                "block_when_pii": true,
                "blocked_actions": ["dangerous_tool"]
            }),
        )
        .expect("policy evaluation should succeed");

    assert!(!decision.allow);
    assert_eq!(decision.reason, "pii_detected");
}

#[test]
fn test_policy_gate_marks_approval_actions() {
    let node = PolicyPrivacyGateNode;

    let decision = node
        .evaluate_policy(
            "external_send",
            false,
            &json!({
                "approval_actions": ["external_send"],
                "block_when_pii": true
            }),
        )
        .expect("policy evaluation should succeed");

    assert!(decision.allow);
    assert!(decision.requires_approval);
}

#[test]
fn test_policy_gate_rejects_invalid_config() {
    let node = PolicyPrivacyGateNode;

    let err = node
        .validate(&json!({ "block_when_pii": "yes" }))
        .expect_err("non-boolean config should fail");
    assert!(err.to_string().contains("block_when_pii"));

    let err = node
        .validate(&json!({ "blocked_actions": ["ok", 1] }))
        .expect_err("non-string blocked action should fail");
    assert!(err.to_string().contains("blocked_actions"));
}

#[test]
fn test_policy_gate_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.policy_privacy_gate")
        .expect("policy gate should be registered");
    assert_eq!(node.spec().type_id, "core.ai.policy_privacy_gate");
}

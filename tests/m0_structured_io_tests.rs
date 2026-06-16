use serde_json::json;
use swanflow::nodes::ai::contracts::StructuredIoContractNode;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeBehavior;

#[test]
fn test_structured_io_contract_rejects_missing_required_fields() {
    let node = StructuredIoContractNode;
    let err = node
        .validate(&json!({}))
        .expect_err("should fail without required_fields");
    assert!(err.to_string().contains("required_fields"));
}

#[test]
fn test_structured_io_contract_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.structured_io_contract")
        .expect("node should be registered");
    assert_eq!(node.spec().type_id, "core.ai.structured_io_contract");
}

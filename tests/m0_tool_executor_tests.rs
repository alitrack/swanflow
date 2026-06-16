use swanflow::nodes::ai::tool_executor::ToolExecutorNode;
use swanflow::nodes::registry::NodeRegistry;
use serde_json::json;

#[test]
fn test_tool_executor_normalizes_success_payload() {
    let node = ToolExecutorNode;
    let normalized = node
        .normalize_payload("web.search", &json!({"result": {"items": [1, 2]}}), 37)
        .expect("normalization should succeed");

    assert_eq!(normalized["status"], "success");
    assert_eq!(normalized["tool_name"], "web.search");
    assert_eq!(normalized["latency_ms"], 37);
    assert_eq!(normalized["data"]["items"][0], 1);
    assert!(normalized["error"].is_null());
}

#[test]
fn test_tool_executor_normalizes_error_payload() {
    let node = ToolExecutorNode;
    let normalized = node
        .normalize_payload("db.query", &json!({"error": "timeout"}), 142)
        .expect("normalization should succeed");

    assert_eq!(normalized["status"], "error");
    assert_eq!(normalized["tool_name"], "db.query");
    assert_eq!(normalized["latency_ms"], 142);
    assert_eq!(normalized["error"], "timeout");
    assert!(normalized["data"].is_null());
}

#[test]
fn test_tool_executor_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.tool_executor")
        .expect("tool executor should be registered");
    assert_eq!(node.spec().type_id, "core.ai.tool_executor");
}

use rustflow::nodes::ai::privacy::PiiDetectorNode;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeBehavior;
use serde_json::json;

#[test]
fn test_pii_detector_detects_common_sensitive_patterns() {
    let node = PiiDetectorNode;
    let input = "Contact alice@example.com or +1 (415) 555-1234. SSN: 123-45-6789";

    let result = node
        .detect_pii(input, &json!({ "min_confidence": 0.0 }))
        .expect("PII detection should succeed");

    let categories: Vec<String> = result.matches.iter().map(|m| m.pii_type.clone()).collect();
    assert!(categories.contains(&"email".to_string()));
    assert!(categories.contains(&"phone".to_string()));
    assert!(categories.contains(&"ssn".to_string()));
}

#[test]
fn test_pii_detector_filters_by_category() {
    let node = PiiDetectorNode;
    let input = "Email bob@example.com and phone 415-555-9988";

    let result = node
        .detect_pii(input, &json!({ "categories": ["email"], "min_confidence": 0.0 }))
        .expect("PII detection should succeed");

    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].pii_type, "email");
}

#[test]
fn test_pii_detector_rejects_invalid_config() {
    let node = PiiDetectorNode;

    let err = node
        .validate(&json!({ "min_confidence": 1.2 }))
        .expect_err("invalid confidence should fail");
    assert!(err.to_string().contains("min_confidence"));

    let err = node
        .validate(&json!({ "categories": ["email", 42] }))
        .expect_err("non-string category should fail");
    assert!(err.to_string().contains("categories"));
}

#[test]
fn test_pii_detector_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.pii_detector")
        .expect("PII detector should be registered");
    assert_eq!(node.spec().type_id, "core.ai.pii_detector");
}

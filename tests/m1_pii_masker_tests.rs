use swanflow::nodes::ai::privacy::PiiMaskerNode;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeBehavior;
use serde_json::json;

#[test]
fn test_pii_masker_masks_and_restores_text() {
    let node = PiiMaskerNode;
    let input = "Reach me at alice@example.com or 415-555-1212";

    let masked = node
        .mask_pii(
            input,
            &json!({ "token_prefix": "MASK", "min_confidence": 0.0 }),
        )
        .expect("masking should succeed");

    assert_ne!(masked.masked_text, input);
    assert!(!masked.masked_text.contains("alice@example.com"));
    assert!(!masked.masked_text.contains("415-555-1212"));
    assert_eq!(masked.replacements.len(), 2);

    let restored = node.demask_pii(&masked.masked_text, &masked.replacements);
    assert_eq!(restored, input);
}

#[test]
fn test_pii_masker_respects_category_filter() {
    let node = PiiMaskerNode;
    let input = "Email bob@example.com and call 415-555-9988";

    let masked = node
        .mask_pii(
            input,
            &json!({ "categories": ["email"], "token_prefix": "PII", "min_confidence": 0.0 }),
        )
        .expect("masking should succeed");

    assert!(masked.masked_text.contains("415-555-9988"));
    assert!(!masked.masked_text.contains("bob@example.com"));
    assert_eq!(masked.replacements.len(), 1);
    assert_eq!(masked.replacements[0].pii_type, "email");
}

#[test]
fn test_pii_masker_rejects_invalid_config() {
    let node = PiiMaskerNode;

    let err = node
        .validate(&json!({ "token_prefix": "" }))
        .expect_err("empty token prefix should fail");
    assert!(err.to_string().contains("token_prefix"));
}

#[test]
fn test_pii_masker_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.pii_masker")
        .expect("PII masker should be registered");
    assert_eq!(node.spec().type_id, "core.ai.pii_masker");
}

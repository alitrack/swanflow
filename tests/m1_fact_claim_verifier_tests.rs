use swanflow::nodes::ai::verification::FactClaimVerifierNode;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeBehavior;
use serde_json::json;

#[test]
fn test_fact_claim_verifier_scores_and_verdicts() {
    let node = FactClaimVerifierNode;
    let claims = vec![
        "Rust was created by Mozilla".to_string(),
        "Saturn is made of cheese".to_string(),
    ];
    let evidences = vec![
        "Rust was originally sponsored by Mozilla Research.".to_string(),
        "Tokyo is the capital of Japan in Asia.".to_string(),
    ];

    let results = node
        .verify_claims(&claims, &evidences, &json!({ "support_threshold": 0.45 }))
        .expect("verification should succeed");

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].claim, claims[0]);
    assert_eq!(results[0].verdict, "supported");
    assert!(results[0].score >= 0.45);

    assert_eq!(results[1].claim, claims[1]);
    assert_eq!(results[1].verdict, "not_supported");
    assert!(results[1].score < 0.45);
}

#[test]
fn test_fact_claim_verifier_handles_missing_evidence() {
    let node = FactClaimVerifierNode;
    let claims = vec!["A claim without evidence".to_string()];

    let results = node
        .verify_claims(
            &claims,
            &[],
            &json!({ "support_threshold": 0.5, "require_evidence": true }),
        )
        .expect("verification should succeed");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].verdict, "insufficient_evidence");
    assert_eq!(results[0].score, 0.0);
}

#[test]
fn test_fact_claim_verifier_rejects_invalid_threshold() {
    let node = FactClaimVerifierNode;
    let err = node
        .validate(&json!({ "support_threshold": 1.5 }))
        .expect_err("invalid threshold should fail");
    assert!(err.to_string().contains("support_threshold"));
}

#[test]
fn test_fact_claim_verifier_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.fact_claim_verifier")
        .expect("fact claim verifier should be registered");
    assert_eq!(node.spec().type_id, "core.ai.fact_claim_verifier");
}

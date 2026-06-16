use serde_json::json;
use swanflow::nodes::ai::retrieval::EmbeddingGeneratorNode;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeBehavior;

#[test]
fn test_embedding_generator_outputs_expected_shape_and_determinism() {
    let node = EmbeddingGeneratorNode;
    let input = vec![
        "rustflow embeddings".to_string(),
        "rustflow embeddings".to_string(),
        "different text".to_string(),
    ];

    let embeddings = node
        .generate_embeddings(&input, &json!({ "embedding_dim": 16, "normalize": true }))
        .expect("embedding generation should succeed");

    assert_eq!(embeddings.len(), 3);
    assert_eq!(embeddings[0].len(), 16);
    assert_eq!(embeddings[1].len(), 16);
    assert_eq!(embeddings[2].len(), 16);

    assert_eq!(embeddings[0], embeddings[1]);
    assert_ne!(embeddings[0], embeddings[2]);

    let norm = embeddings[0].iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((norm - 1.0).abs() < 1e-4);
}

#[test]
fn test_embedding_generator_rejects_zero_dimension() {
    let node = EmbeddingGeneratorNode;
    let err = node
        .validate(&json!({ "embedding_dim": 0 }))
        .expect_err("zero embedding_dim should fail");
    assert!(err.to_string().contains("embedding_dim"));
}

#[test]
fn test_embedding_generator_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.embedding_generator")
        .expect("embedding generator should be registered");
    assert_eq!(node.spec().type_id, "core.ai.embedding_generator");
}

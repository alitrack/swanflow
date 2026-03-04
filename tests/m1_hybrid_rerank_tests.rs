use rustflow::nodes::ai::retrieval::{
    HybridRetrieverNode, RerankBackend, RetrievalCandidate, RerankerNode,
};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeBehavior;
use serde_json::json;

#[test]
fn test_hybrid_retriever_fuses_sparse_and_semantic_scores() {
    let node = HybridRetrieverNode;
    let sparse = vec![
        RetrievalCandidate {
            source_id: "a".to_string(),
            content: "alpha doc".to_string(),
            score: 0.9,
        },
        RetrievalCandidate {
            source_id: "b".to_string(),
            content: "beta doc".to_string(),
            score: 0.5,
        },
    ];
    let semantic = vec![
        RetrievalCandidate {
            source_id: "b".to_string(),
            content: "beta doc".to_string(),
            score: 0.9,
        },
        RetrievalCandidate {
            source_id: "c".to_string(),
            content: "gamma doc".to_string(),
            score: 0.7,
        },
    ];

    let fused = node
        .fuse_candidates(sparse, semantic, &json!({ "alpha": 0.4, "top_k": 2 }))
        .expect("hybrid fusion should succeed");

    assert_eq!(fused.len(), 2);
    assert_eq!(fused[0].source_id, "b");
    assert_eq!(fused[1].source_id, "a");
    assert!(fused[0].score >= fused[1].score);
}

#[test]
fn test_reranker_reorders_candidates_with_query_overlap() {
    let node = RerankerNode;
    let ranked = node
        .rerank_candidates(
            "rust tutorial",
            vec![
                RetrievalCandidate {
                    source_id: "doc-1".to_string(),
                    content: "python tutorial".to_string(),
                    score: 0.80,
                },
                RetrievalCandidate {
                    source_id: "doc-2".to_string(),
                    content: "rust async tutorial".to_string(),
                    score: 0.75,
                },
            ],
            &json!({ "query_term_boost": 0.12, "top_k": 1 }),
        )
        .expect("reranking should succeed");

    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].source_id, "doc-2");
}

#[test]
fn test_hybrid_and_reranker_nodes_are_registered() {
    let registry = NodeRegistry::default();

    let hybrid = registry
        .get_node("core.ai.hybrid_retriever")
        .expect("hybrid retriever should be registered");
    assert_eq!(hybrid.spec().type_id, "core.ai.hybrid_retriever");

    let reranker = registry
        .get_node("core.ai.reranker")
        .expect("reranker should be registered");
    assert_eq!(reranker.spec().type_id, "core.ai.reranker");
}

#[test]
fn test_hybrid_rejects_invalid_alpha() {
    let node = HybridRetrieverNode;
    let err = node
        .validate(&json!({ "alpha": 2.0 }))
        .expect_err("alpha out of range should fail");
    assert!(err.to_string().contains("alpha"));
}

#[test]
fn test_reranker_resolves_backend_variants() {
    let node = RerankerNode;

    assert_eq!(
        node.resolve_backend(&json!({ "backend": "rule" })).unwrap(),
        RerankBackend::Rule
    );
    assert_eq!(
        node.resolve_backend(&json!({ "backend": "model" })).unwrap(),
        RerankBackend::Model
    );
    assert_eq!(
        node.resolve_backend(&json!({ "backend": "llm" })).unwrap(),
        RerankBackend::Llm
    );
}

#[test]
fn test_reranker_rejects_invalid_backend() {
    let node = RerankerNode;
    let err = node
        .validate(&json!({ "backend": "legacy_ranker" }))
        .expect_err("unknown backend should fail");
    assert!(err.to_string().contains("backend"));
}

#[test]
fn test_reranker_supports_model_and_llm_backends() {
    let node = RerankerNode;
    let candidates = vec![
        RetrievalCandidate {
            source_id: "doc-1".to_string(),
            content: "python tutorial".to_string(),
            score: 0.80,
        },
        RetrievalCandidate {
            source_id: "doc-2".to_string(),
            content: "rust async tutorial".to_string(),
            score: 0.75,
        },
    ];

    let ranked_model = node
        .rerank_candidates(
            "rust tutorial",
            candidates.clone(),
            &json!({ "backend": "model", "top_k": 1, "query_term_boost": 0.12 }),
        )
        .expect("model backend rerank should succeed");
    assert_eq!(ranked_model[0].source_id, "doc-2");

    let ranked_llm = node
        .rerank_candidates(
            "rust tutorial",
            candidates,
            &json!({ "backend": "llm", "top_k": 1, "query_term_boost": 0.12 }),
        )
        .expect("llm backend rerank should succeed");
    assert_eq!(ranked_llm[0].source_id, "doc-2");
}

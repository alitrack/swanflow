use rustflow::nodes::ai::retrieval::{VectorIndexWriterNode, VectorRetrieverNode};
use rustflow::nodes::registry::NodeRegistry;

#[test]
fn test_vector_writer_outputs_index_metadata_contract() {
    let node = VectorIndexWriterNode;
    let row = node
        .build_contract_row("idx-a", "source-1", "chunk-1", "hello world", 384, None)
        .expect("writer contract should build");

    assert_eq!(row.index_id, "idx-a");
    assert_eq!(row.source_id, "source-1");
    assert_eq!(row.chunk_id, "chunk-1");
    assert_eq!(row.embedding_dim, 384);
    assert_eq!(row.score, 1.0);
}

#[test]
fn test_vector_retriever_outputs_ranked_context_contract() {
    let node = VectorRetrieverNode;
    let row = node
        .build_contract_row(
            "idx-a",
            "source-2",
            "chunk-8",
            "retrieved text",
            384,
            Some(0.87),
        )
        .expect("retriever contract should build");

    assert_eq!(row.index_id, "idx-a");
    assert_eq!(row.source_id, "source-2");
    assert_eq!(row.chunk_id, "chunk-8");
    assert!((row.score - 0.87).abs() < 1e-6);
}

#[test]
fn test_vector_nodes_are_registered() {
    let registry = NodeRegistry::default();

    let writer = registry
        .get_node("core.ai.vector_index_writer")
        .expect("vector writer should be registered");
    assert_eq!(writer.spec().type_id, "core.ai.vector_index_writer");

    let retriever = registry
        .get_node("core.ai.vector_retriever")
        .expect("vector retriever should be registered");
    assert_eq!(retriever.spec().type_id, "core.ai.vector_retriever");
}

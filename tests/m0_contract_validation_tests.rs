use serde_json::json;
use swanflow::nodes::ai::contracts::{CitationTrackerNode, StructuredIoContractNode};
use swanflow::nodes::ai::retrieval::{
    EmbeddingGeneratorNode, QueryPlanningNode, SemanticChunkerNode, VectorIndexWriterNode,
    VectorRetrieverNode,
};
use swanflow::nodes::ai::tool_executor::ToolExecutorNode;
use swanflow::nodes::io::UniversalReadNode;
use swanflow::nodes::NodeBehavior;

// core.ai.structured_io_contract is a REAL node since UA port batch 2
// (2026-10-16): the 4-layer LLM-output validation gate (sanitize → alias
// map → auto-fix → per-element JSON Schema check, fail-soft drop-and-count
// by default, `strict: true` = fail-fast). The legacy `required_fields`
// config is still honored as a synthesized object-schema shorthand, so the
// shell-era tests below keep their meaning; the new config surface is
// `schema` (+ `alias_map` / `strict` / `input_column`).
#[test]
fn test_structured_io_contract_rejects_empty_required_fields() {
    let node = StructuredIoContractNode;
    let err = node
        .validate(&json!({ "required_fields": [] }))
        .expect_err("empty required_fields should fail");
    assert!(err.to_string().contains("must not be empty"));
}

#[test]
fn test_structured_io_contract_accepts_real_config_surface() {
    let node = StructuredIoContractNode;
    // JSON Schema (the primary config of the converted node) + alias map +
    // strict flag all validate.
    assert!(
        node
            .validate(&json!({
                "schema": {
                    "type": "object",
                    "properties": { "kind": { "enum": ["organization", "person"] } },
                    "required": ["kind"]
                },
                "alias_map": { "组织": "organization" },
                "strict": false
            }))
            .is_ok(),
        "schema + alias_map + strict must be accepted"
    );
    // Neither `schema` nor `required_fields` → config error naming both.
    let err = node
        .validate(&json!({}))
        .expect_err("no schema and no required_fields should fail");
    let msg = err.to_string();
    assert!(msg.contains("schema") && msg.contains("required_fields"), "got: {msg}");
    // `schema` must be a JSON object, not a scalar.
    assert!(node.validate(&json!({ "schema": "object" })).is_err());
}

#[test]
fn test_citation_tracker_rejects_zero_max_citations() {
    let node = CitationTrackerNode;
    let err = node
        .validate(&json!({ "max_citations": 0 }))
        .expect_err("zero max_citations should fail");
    assert!(err.to_string().contains("greater than zero"));
}

#[test]
fn test_query_planning_rejects_zero_max_subqueries() {
    let node = QueryPlanningNode;
    let err = node
        .validate(&json!({ "max_subqueries": 0 }))
        .expect_err("zero max_subqueries should fail");
    assert!(err.to_string().contains("greater than zero"));
}

#[test]
fn test_semantic_chunker_rejects_overlap_not_less_than_chunk_size() {
    let node = SemanticChunkerNode;
    let err = node
        .validate(&json!({ "chunk_size": 4, "overlap": 4 }))
        .expect_err("overlap >= chunk_size should fail");
    assert!(err.to_string().contains("smaller than chunk_size"));
}

#[test]
fn test_vector_nodes_reject_zero_embedding_dim() {
    let embedder = EmbeddingGeneratorNode;
    let embedder_err = embedder
        .validate(&json!({ "embedding_dim": 0 }))
        .expect_err("embedding generator embedding_dim=0 should fail");
    assert!(embedder_err.to_string().contains("greater than zero"));

    let writer = VectorIndexWriterNode;
    let writer_err = writer
        .validate(&json!({ "embedding_dim": 0 }))
        .expect_err("writer embedding_dim=0 should fail");
    assert!(writer_err.to_string().contains("greater than zero"));

    let retriever = VectorRetrieverNode;
    let retriever_err = retriever
        .validate(&json!({ "embedding_dim": 0 }))
        .expect_err("retriever embedding_dim=0 should fail");
    assert!(retriever_err.to_string().contains("greater than zero"));
}

#[test]
fn test_tool_executor_rejects_empty_tool_name() {
    let node = ToolExecutorNode;
    let err = node
        .validate(&json!({ "tool_name": "   " }))
        .expect_err("empty tool_name should fail");
    assert!(err.to_string().contains("must not be empty"));
}

#[test]
fn test_universal_resolver_rejects_invalid_source_type() {
    let err = UniversalReadNode::resolve_route_metadata(&json!({
        "source_type": "blob",
        "source": "demo.csv"
    }))
    .expect_err("invalid source_type should fail");
    assert!(err.to_string().contains("auto|url|file|table"));
}

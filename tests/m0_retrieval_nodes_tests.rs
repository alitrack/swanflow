use rustflow::nodes::ai::retrieval::{QueryPlanningNode, SemanticChunkerNode};
use rustflow::nodes::registry::NodeRegistry;
use serde_json::json;

#[test]
fn test_query_planning_splits_goal_into_ranked_subqueries() {
    let node = QueryPlanningNode;
    let planned = node
        .plan_queries(
            "Find GDP growth and inflation trend in China; compare with US.",
            &json!({"max_subqueries": 3}),
        )
        .expect("planning should succeed");

    assert_eq!(planned.len(), 3);
    assert!(planned[0].rank < planned[1].rank);
    assert!(planned.iter().any(|item| item.query.to_lowercase().contains("gdp")));
    assert!(planned.iter().any(|item| item.query.to_lowercase().contains("inflation")));
}

#[test]
fn test_semantic_chunker_respects_chunk_and_overlap_limits() {
    let node = SemanticChunkerNode;
    let chunks = node
        .chunk_text(
            "one two three four five six seven",
            &json!({"chunk_size": 3, "overlap": 1}),
        )
        .expect("chunking should succeed");

    assert_eq!(chunks, vec![
        "one two three".to_string(),
        "three four five".to_string(),
        "five six seven".to_string(),
    ]);
}

#[test]
fn test_retrieval_nodes_are_registered() {
    let registry = NodeRegistry::default();
    let planner = registry
        .get_node("core.ai.query_planning")
        .expect("query planner should be registered");
    assert_eq!(planner.spec().type_id, "core.ai.query_planning");

    let chunker = registry
        .get_node("core.ai.semantic_chunker")
        .expect("semantic chunker should be registered");
    assert_eq!(chunker.spec().type_id, "core.ai.semantic_chunker");
}

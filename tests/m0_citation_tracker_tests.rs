use rustflow::nodes::ai::contracts::{CitationItem, CitationTrackerNode};
use rustflow::nodes::registry::NodeRegistry;
use serde_json::json;

#[test]
fn test_citation_tracker_emits_source_mapping() {
    let node = CitationTrackerNode;
    let citations = vec![
        CitationItem {
            source_id: "doc-2".to_string(),
            snippet: "second".to_string(),
            score: 0.51,
            start: 12,
            end: 20,
        },
        CitationItem {
            source_id: "doc-1".to_string(),
            snippet: "first".to_string(),
            score: 0.92,
            start: 0,
            end: 10,
        },
        CitationItem {
            source_id: "doc-3".to_string(),
            snippet: "third".to_string(),
            score: 0.80,
            start: 21,
            end: 30,
        },
    ];

    let ranked = node
        .rank_citations(citations, &json!({"max_citations": 2}))
        .expect("ranking should succeed");

    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].source_id, "doc-1");
    assert_eq!(ranked[1].source_id, "doc-3");
}

#[test]
fn test_citation_tracker_is_registered() {
    let registry = NodeRegistry::default();
    let node = registry
        .get_node("core.ai.citation_tracker")
        .expect("citation tracker should be registered");
    assert_eq!(node.spec().type_id, "core.ai.citation_tracker");
}

use swanflow::nodes::ai::contracts::CitationItem;

#[test]
fn test_contract_types_are_serializable() {
    let v = CitationItem {
        source_id: "s1".to_string(),
        snippet: "x".to_string(),
        score: 0.9,
        start: 0,
        end: 1,
    };
    let s = serde_json::to_string(&v).unwrap();
    assert!(s.contains("source_id"));
}

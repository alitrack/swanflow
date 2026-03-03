use rustflow::nodes::ai::graph::ResearchState;

#[test]
fn test_research_state_deduplication() {
    let mut state = ResearchState::new(2);
    
    let url = "https://example.com".to_string();
    let query = "rust data science".to_string();
    
    assert!(!state.is_visited_url(&url));
    assert!(!state.is_visited_query(&query));
    
    state.visit_url(url.clone());
    state.visit_query(query.clone());
    
    assert!(state.is_visited_url(&url));
    assert!(state.is_visited_query(&query));
}

#[test]
fn test_research_state_depth_limit() {
    let mut state = ResearchState::new(2);
    
    assert!(state.should_continue());
    
    state.current_depth = 1;
    assert!(state.should_continue());
    
    state.current_depth = 2;
    assert!(!state.should_continue());
}

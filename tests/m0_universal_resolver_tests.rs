use serde_json::json;
use swanflow::nodes::io::UniversalReadNode;

#[test]
fn test_universal_resolver_routes_url_file_table_sources() {
    let url_route = UniversalReadNode::resolve_route_metadata(&json!({
        "source_type": "auto",
        "source": "https://example.com/data.csv"
    }))
    .expect("url route should resolve");
    assert_eq!(url_route["resolved_source_type"], "url");

    let file_route = UniversalReadNode::resolve_route_metadata(&json!({
        "source_type": "auto",
        "source": "./data/users.csv"
    }))
    .expect("file route should resolve");
    assert_eq!(file_route["resolved_source_type"], "file");

    let table_route = UniversalReadNode::resolve_route_metadata(&json!({
        "source_type": "table",
        "source": "SELECT 1 AS id"
    }))
    .expect("table route should resolve");
    assert_eq!(table_route["resolved_source_type"], "table");
}

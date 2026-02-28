use rustflow::nodes::registry::NodeRegistry;
use serde_json::json;
use std::sync::Arc;

#[test]
fn test_duckdb_read_node_validation() {
    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();

    let node = registry.get_node("core.io.duckdb_read").unwrap();
    let valid_config = json!({
        "path": "test.db",
        "query": "SELECT * FROM users"
    });
    assert!(node.validate(&valid_config).is_ok());

    let invalid_config = json!({
        "path": "test.db"
    });
    assert!(node.validate(&invalid_config).is_err());
}

#[test]
fn test_duckdb_write_node_validation() {
    let mut registry = NodeRegistry::new();
    registry.register_core_nodes();

    let node = registry.get_node("core.io.duckdb_write").unwrap();
    let valid_config = json!({
        "path": "test.db",
        "table": "results"
    });
    assert!(node.validate(&valid_config).is_ok());

    let invalid_config = json!({
        "path": "test.db"
    });
    assert!(node.validate(&invalid_config).is_err());
}
